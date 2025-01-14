//comment

use axum::{
    extract::Query,
    routing::get,
    Router,
    Json,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use csv_async::AsyncReader;
use futures::StreamExt;

#[derive(Debug, Deserialize)]
struct QueryParams {
    cep: String,
    cnae: String,
}

#[derive(Debug, Serialize, Clone)]
struct Record {
    nomefantasia: String,
    tipologradouro: String,
    logradouro: String,
    numero: String,
    complemento: String,
    bairro: String,
    cep: String,
    distance: i32,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(handle_query));

    println!("Server running on http://localhost:3000");
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_query(Query(params): Query<QueryParams>) -> Json<Vec<Record>> {
    let base_cep = params.cep.parse::<i32>().unwrap_or(0);
    let cnae = params.cnae;
    
    // List of CSV files to process
    let files = vec![
        "file1.csv", "file2.csv", "file3.csv", "file4.csv", "file5.csv",
        "file6.csv", "file7.csv", "file8.csv", "file9.csv", "file10.csv"
    ];
    
    let mut tasks = Vec::new();
    
    // Process each file concurrently
    for file_name in files {
        let cnae = cnae.clone();
        let task = tokio::spawn(async move {
            process_file(file_name, &cnae, base_cep).await
        });
        tasks.push(task);
    }
    
    // Collect and combine results
    let mut all_records = Vec::new();
    for task in tasks {
        if let Ok(mut records) = task.await.unwrap_or_else(|_| Vec::new()) {
            all_records.append(&mut records);
        }
    }
    
    // Sort by distance and take top 5
    all_records.sort_by_key(|r| r.distance);
    all_records.truncate(5);
    
    Json(all_records)
}

async fn process_file(file_name: &str, cnae: &str, base_cep: i32) -> Vec<Record> {
    let mut records = Vec::new();
    
    let file = match File::open(file_name).await {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    
    let mut rdr = csv_async::AsyncReader::from_reader(file);
    let mut record = csv_async::StringRecord::new();
    
    while rdr.read_record(&mut record).await.unwrap_or(false) {
        // Extract fields
        let current_cep = record.get(6).unwrap_or("").to_string();
        if !current_cep.starts_with("71") {
            continue;
        }
        
        let cnae1 = record.get(7).unwrap_or("");
        let cnae2 = record.get(8).unwrap_or("");
        let situacao_cadastral = record.get(9).unwrap_or("");
        let nome_fantasia = record.get(0).unwrap_or("");
        
        // Apply filters
        if (nome_fantasia.is_empty() 
            || situacao_cadastral != "2"
            || (!cnae1.contains(cnae) && !cnae2.contains(cnae))) {
            continue;
        }
        
        // Calculate distance
        let current_cep_num = current_cep.parse::<i32>().unwrap_or(0);
        let distance = (current_cep_num - base_cep).abs();
        
        records.push(Record {
            nomefantasia: nome_fantasia.to_string(),
            tipologradouro: record.get(1).unwrap_or("").to_string(),
            logradouro: record.get(2).unwrap_or("").to_string(),
            numero: record.get(3).unwrap_or("").to_string(),
            complemento: record.get(4).unwrap_or("").to_string(),
            bairro: record.get(5).unwrap_or("").to_string(),
            cep: current_cep,
            distance,
        });
    }
    
    records
}