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

struct PgAppState {
    pool: Pool,
}

#[post("/rarbg/batch_pq")]
async fn get_items_batch_pq(
    data: web::Data<PgAppState>,
    search_request: web::Json<SearchRequest>,
) -> impl Responder {
    let client = data.pool.get().await.unwrap();
    let titles = &search_request.titles;

    // 构建动态SQL查询，确保每个标题都能被正确处理
    let mut query_str = String::from("SELECT hash, title, dt, cat, size FROM items WHERE ");
    // let processed_titles: Vec<String> = titles
    //     .iter()
    //     .map(|title| {
    //         format!(
    //             "%{}.%",
    //             title.split_whitespace().collect::<Vec<_>>().join(".%.")
    //         )
    //     })
    //     .collect();

    for (index, _) in titles.iter().enumerate() {
        if index > 0 {
            query_str.push_str(" OR ");
        }
        query_str.push_str(&format!("lower(title) LIKE lower(${})", index + 1));
    }

    query_str.push_str(" ORDER BY title ASC LIMIT 10000");

    let stmt = client.prepare(&query_str).await.unwrap();

    // 将 Vec 转换成切片，并且确保每个元素都实现了 ToSql + Sync
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
async fn handle_post(data: web::Json<ImageData>) -> impl Responder {
    let title = &data.title;
    let page_url = &data.page_url;
    let base_dir = Path::new("C:\\Users\\aa\\Desktop\\zup");
    let dir_path = base_dir.join(title);

    if !dir_path.exists() {
        fs::create_dir_all(&dir_path).expect("Failed to create directory");
    }

    let semaphore = Arc::new(Semaphore::new(8));
    let mut joinset = JoinSet::new();

    for (index, url) in data.img_url_array.iter().enumerate() {
        let file_name = format!("{:04}.jpg", index + 1);
        let file_path = dir_path.join(&file_name);
        let url = url.clone();
        let semaphore = semaphore.clone();

        joinset.spawn(async move {
            let _permit = semaphore.clone().acquire_owned().await.unwrap(); // 持有信号量许可

            if file_path.exists() {
                return Ok("existed");
            }

            match download_image(&url, &file_path).await {
                Ok(_) => Ok("OK"),
                Err(e) => {
                    eprintln!("Failed to download {}: {}", url, e);
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
            Ok(Err(url)) => failed_urls.push(url), // 下载失败
            Err(e) => eprintln!("Task panicked: {:?}", e), // 任务崩溃
        }
    }
    println!("{}\n已完成！", title);

    if !failed_urls.is_empty() {
        let html_content = format!(
            r#"<html>
            <body>
                <h1><a href="{}">{}</a></h1>
                <ul>
                    {}
                </ul>
            </body>
        </html>"#,
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

async fn download_image(url: &str, path: &Path) -> Result<(), String> {
    let client = Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| e.to_string())?;

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

    let pg_app_state = web::Data::new(PgAppState { pool: pg_pool });

    HttpServer::new(move || {
        App::new()
            .app_data(pg_app_state.clone())
            .service(handle_post)
            .service(get_items_batch_pq)
    })
    .bind("127.0.0.1:46644")?
    .run()
    .await
}
