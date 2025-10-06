use std::fs;
use mongodb::{
    bson::{Document},
    change_stream::event::ChangeStreamEvent,
    Client,
};

use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32};
use std::sync::atomic::Ordering::SeqCst;
use futures::stream::StreamExt;
use tokio;

const FILE_PATH: &str = "./rq_log/";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let conn_string = String::from("mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true");

    println!("Подключение к {}", conn_string);

    let client = Client::with_uri_str(&conn_string).await?;
    let atomic_counter: AtomicI32 = AtomicI32::new(1);

    let database = Arc::new(client.database("mock_db"));

    if !fs::metadata(FILE_PATH).is_ok() {
        fs::create_dir(FILE_PATH)?;
    }

    tokio::spawn(watch_cl_mock_report(Arc::clone(&database), atomic_counter));

    tokio::signal::ctrl_c().await?;
    println!("Остановка...");

    Ok(())
}

async fn next_number(atomic_i32: &mut AtomicI32) -> i32 {
    atomic_i32.fetch_add(1, SeqCst)
}

async fn watch_cl_mock_report(database: Arc<mongodb::Database>, mut a_c: AtomicI32) {
    let collection = database.collection::<Document>("mock_report");

    match collection.watch(None, None).await {
        Ok(mut change_stream) => {
            println!("Слушаются измения в коллекции 'mock_report'");

            while let Some(change) = change_stream.next().await {
                match change {
                    Ok(change_event) => {
                        handle_change_event("mock_report", change_event, next_number(&mut a_c).await);
                    }
                    Err(e) => {
                        eprintln!("Ошибка в change stream 'mock_report': {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Не удалось создать change stream для 'mock_report': {}", e);
        }
    }
}

fn handle_change_event(collection_name: &str, change: ChangeStreamEvent<Document>, num: i32) {
    println!("\nИзменение в коллекции '{}':", collection_name);
    println!("   Operation: {:#?}", change.operation_type);

    match change.operation_type {
        mongodb::change_stream::event::OperationType::Insert => {
            if let Some(doc) = change.full_document {
                println!("New Doc: {:#?}", doc);

                write_info(&doc, num).expect("Паника в insert");
            }
        }
        mongodb::change_stream::event::OperationType::Update => {
            if let Some(update_desc) = change.update_description {
                println!("Fields updated: {:?}", update_desc.updated_fields);
                write_info(&update_desc.updated_fields, num).expect("Паника в update");
            }
            println!("Doc id: {:?}", change.document_key);
        }
        mongodb::change_stream::event::OperationType::Delete => {
            match change.document_key {
                Some(key) => {
                    println!("Doc deleted: {:?}", key);
                    write_info(&key, num).expect("Паника в delete");
                },
                None => {},
            }
        }
        mongodb::change_stream::event::OperationType::Replace => {
            if let Some(doc) = change.full_document {
                println!("swapped for: {}", doc);
                write_info(&doc, num).expect("Паника в replace");
            }
        }
        _ => {
            let other_operation = change.operation_type;

            println!("Не сериализуется");
            println!("Другая операция: {:?}", other_operation);
        }
    }
    println!("   ---");
}

fn write_info(d: &Document, num: i32) -> Result<(), Box<dyn Error>> {
    let pretty_json = to_pretty_json(&d)?;
    let bytes_pretty_json = pretty_json.as_bytes();

    let leaf_path = format!("{}{}.json", FILE_PATH, num);

    let _bytes_written = File::create(&leaf_path)?.write(bytes_pretty_json);

    Ok(())
}

fn to_pretty_json(d: &Document) -> Result<String, Box<dyn Error>> {
    let json_string = serde_json::to_string_pretty(&d)?;
    Ok(json_string)
}