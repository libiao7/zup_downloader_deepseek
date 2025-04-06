use actix_web::{App, HttpResponse, HttpServer, Responder, post, web};
use deadpool_postgres::{Config, Pool, Runtime};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::copy;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_postgres::NoTls;

#[derive(Debug, Serialize)]
struct Item {
    hash: String,
    title: String,
    dt: String,
    cat: String,
    size: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ImageData {
    title: String,
    img_url_array: Vec<String>,
    page_url: String,
}

#[derive(Deserialize, Debug)]
struct SearchRequest {
    titles: Vec<String>,
}

struct AppState {
    pg_pool: Pool,
    http_client: Client,                // 共享的 reqwest::Client
    download_semaphore: Arc<Semaphore>, // 全局下载信号量
}

#[post("/rarbg/batch_pq")]
async fn get_items_batch_pq(
    data: web::Data<AppState>,
    search_request: web::Json<SearchRequest>,
) -> impl Responder {
    let client = data.pg_pool.get().await.unwrap();
    let titles = &search_request.titles;

    let mut query_str = String::from("SELECT hash, title, dt, cat, size FROM items WHERE ");
    for (index, _) in titles.iter().enumerate() {
        if index > 0 {
            query_str.push_str(" OR ");
        }
        query_str.push_str(&format!("lower(title) LIKE lower(${})", index + 1));
    }
    query_str.push_str(" ORDER BY title ASC LIMIT 10000");

    let stmt = client.prepare(&query_str).await.unwrap();
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = titles
        .iter()
        .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let rows = client.query(&stmt, &params.as_slice()).await.unwrap();
    let items: Vec<Item> = rows
        .iter()
        .map(|row| Item {
            hash: row.get(0),
            title: row.get(1),
            dt: row.get(2),
            cat: row.get(3),
            size: row.get(4),
        })
        .collect();

    HttpResponse::Ok().json(items)
}

#[post("/zup")]
async fn handle_post(data: web::Json<ImageData>, app_state: web::Data<AppState>) -> impl Responder {
    let title = &data.title;
    let page_url = &data.page_url;
    let base_dir = Path::new("C:\\Users\\aa\\Desktop\\zup");
    let dir_path = base_dir.join(title);

    if !dir_path.exists() {
        fs::create_dir_all(&dir_path).expect("Failed to create directory");
    }

    let mut joinset = JoinSet::new();

    for (index, url) in data.img_url_array.iter().enumerate() {
        let file_name = format!("{:04}.jpg", index + 1);
        let file_path = dir_path.join(&file_name);
        let url = url.clone();
        let semaphore = app_state.download_semaphore.clone(); // 使用全局信号量
        let task_client = app_state.http_client.clone();
        joinset.spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();

            if file_path.exists() {
                return Ok("existed");
            }

            match download_image(task_client, &url, &file_path).await {
                Ok(_) => Ok("OK"),
                Err(e) => {
                    eprintln!("e: {}: {}", e, url);
                    Err(url)
                }
            }
        });
    }

    let total_count = data.img_url_array.len();
    let mut success_count = 0;
    let mut failed_urls = Vec::new();

    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(Ok(t)) => {
                success_count += 1;
                println!("{t}: {total_count}---{success_count}");
            }
            Ok(Err(url)) => failed_urls.push(url),
            Err(e) => eprintln!("Task panicked: {:?}", e),
        }
    }
    println!("{}\n已完成！", title);

    if !failed_urls.is_empty() {
        let html_content = format!(
            r#"<html><body><h1><a href="{}">{}</a></h1><ul>{}</ul></body></html>"#,
            page_url,
            title,
            failed_urls
                .iter()
                .map(|url| format!("<li><a href=\"{}\">{}</a></li>", url, url))
                .collect::<Vec<_>>()
                .join("")
        );
        fs::write(dir_path.join("failed_downloads.html"), html_content)
            .expect("Failed to write HTML file");
    } else {
        let failed_file_path = dir_path.join("failed_downloads.html");
        if failed_file_path.exists() {
            fs::remove_file(failed_file_path).expect("Failed to delete failed_downloads.html");
        }
    }

    let _ = Command::new("C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe")
        .arg(dir_path.to_str().unwrap())
        .output();

    HttpResponse::Ok().body(format!("{}\n已完成！", title))
}

async fn download_image(client: Client, url: &str, path: &Path) -> Result<(), String> {
    let response = client.get(url).send().await.map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("Failed to download image: {}", response.status()));
    }

    let content = response.bytes().await.map_err(|e| e.to_string())?;
    if content.is_empty() {
        return Err("Downloaded file is empty".to_string());
    }

    let mut file = fs::File::create(path).map_err(|e| e.to_string())?;
    copy(&mut content.as_ref(), &mut file).map_err(|e| e.to_string())?;

    Ok(())
}

async fn init_pool() -> Pool {
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("4545".to_string());
    cfg.dbname = Some("rarbg".to_string());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let pg_pool = init_pool().await;
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    let app_state = web::Data::new(AppState {
        pg_pool,
        http_client,                                     // 共享的 Client
        download_semaphore: Arc::new(Semaphore::new(8)), // 全局8个并发许可
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone()) // 克隆 web::Data (Arc)
            .service(handle_post)
            .service(get_items_batch_pq)
    })
    .bind("127.0.0.1:46644")?
    .run()
    .await
}
