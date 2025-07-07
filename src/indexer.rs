use crate::crawler::get_links;

use csv::Writer;
use futures::{stream, StreamExt};
use percent_encoding::NON_ALPHANUMERIC;
use rayon::prelude::*;
use reqwest::Client;
use rust_stemmers::{Algorithm, Stemmer};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json};
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Result as IoResult, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::{remove_file, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{self, timeout};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Website {
    doc_id: usize,
    url: String,
    title: String,
    description: String,
    words: Vec<String>,
}

fn load_word_list(filename: &str) -> HashSet<String> {
    let word_list = fs::read_to_string(filename).expect("Failed to read word list");
    word_list.lines().map(|w| w.to_lowercase()).collect()
}

fn load_stop_word_list(filename: &str) -> HashSet<String> {
    let stop_word_list = fs::read_to_string(filename).expect("Failed to read word list");
    stop_word_list.lines().map(|w| w.to_lowercase()).collect()
}

fn update_loading_bar(total: usize, done: usize, text: &str) {
    let bar_length = 30;
    let percent = (done as f64 / total as f64) * 100.0;
    let num_equals = (percent / (100.0 / bar_length as f64)).round() as usize;
    let num_spaces = bar_length - num_equals;

    if percent == 100.0 {
        print!(
            "\r{}: [{}] {:.3}%\n",
            text,
            "=".repeat(bar_length + 1),
            percent
        );
    } else {
        print!(
            "\r{}: [{}>{}] {:.3}% ",
            text,
            "=".repeat(num_equals),
            " ".repeat(num_spaces),
            percent
        );
    }

    io::stdout().flush().unwrap();
}

fn process_words(
    text: &str,
    word_list: &HashSet<String>,
    stop_word_list: &HashSet<String>,
) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);

    let words: Vec<String> = text
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|c: char| !c.is_alphabetic())
                .to_lowercase()
        })
        .filter(|word| word_list.contains(word) && !stop_word_list.contains(word) && word.len() > 2)
        .map(|word| stemmer.stem(&word).to_string())
        .collect::<Vec<_>>()
        .into_iter()
        .fold((Vec::new(), HashSet::new()), |(mut acc, mut seen), word| {
            if seen.insert(word.clone()) {
                acc.push(word);
            }
            (acc, seen)
        })
        .0;

    words
}

async fn fetch_website_data(
    doc_id: usize,
    url: &Url,
    word_list: &HashSet<String>,
    stop_word_list: &HashSet<String>,
    client: &Client,
) -> Result<Website, ()> {
    let response = match client.get(url.clone()).send().await {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("Request error for {}: {}", url, e);
            return Err(());
        }
    };

    if !response.status().is_success() {
        eprintln!("Non-success status {} for {}", response.status(), url);
        return Err(());
    }

    let body = match response.text().await {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read body for {}: {}", url, e);
            return Err(());
        }
    };

    let document = Html::parse_document(&body);

    let title_selector = Selector::parse("title").unwrap();
    let meta_selector = Selector::parse("meta[name=\"description\"]").unwrap();
    let body_selector = Selector::parse("body").unwrap();

    let title = document
        .select(&title_selector)
        .next()
        .map(|t| t.inner_html())
        .unwrap_or_else(|| "No Title".to_string());

    let body_text = document
        .select(&body_selector)
        .next()
        .map(|b| b.text().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();

    let description = document
        .select(&meta_selector)
        .next()
        .and_then(|meta| meta.value().attr("content").map(|desc| desc.to_string()))
        .unwrap_or_else(|| {
            let words: Vec<&str> = body_text.split_whitespace().collect();
            let first_200_words = words
                .iter()
                .filter(|word| word_list.contains(**word))
                .take(200)
                .copied()
                .collect::<Vec<&str>>()
                .join(" ");
            if first_200_words.trim().is_empty() {
                "No Description".to_string()
            } else {
                first_200_words
            }
        });

    let words = process_words(&body_text, word_list, stop_word_list);

    if words.is_empty() && title == "No Title" && description == "No Description" {
        return Err(());
    }

    Ok(Website {
        doc_id,
        url: url.to_string(),
        title,
        description,
        words,
    })
}

fn build_inverted_index(path: &str) -> IoResult<HashMap<String, HashSet<usize>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut index: HashMap<String, HashSet<usize>> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let website: Website = from_str(&line).unwrap();

        for word in website.words {
            index.entry(word).or_default().insert(website.doc_id);
        }
    }

    Ok(index)
}

fn save_inverted_index(index: &HashMap<String, HashSet<usize>>, filename: &str) -> IoResult<()> {
    let mut writer = Writer::from_path(filename)?;

    writer.write_record(["word", "doc_ids"])?;

    for (word, doc_ids) in index {
        let doc_ids_str = doc_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        writer.write_record([word, &doc_ids_str])?;
    }

    writer.flush()?;
    Ok(())
}

fn save_metadata(path: &str, pageranks: &HashMap<Url, f64>, filename: &str) -> IoResult<()> {
    let input = File::open(path)?;
    let reader = BufReader::new(input);
    let mut writer = Writer::from_path(filename)?;

    writer.write_record(["doc_id", "url", "title", "pagerank", "description"])?;

    for line in reader.lines() {
        let line = line?;
        let website: Website = from_str(&line).unwrap();

        let pagerank = pageranks
            .get(&Url::parse(&website.url).unwrap())
            .unwrap_or(&0.0);

        writer.write_record(&[
            website.doc_id.to_string(),
            website.url,
            website.title,
            format!("{:.6}", pagerank),
            website.description,
        ])?;
    }

    writer.flush()?;
    Ok(())
}

async fn fetch_url(url: &Url, max_retries: usize, client: &Client) -> Option<String> {
    let mut retries = 0;
    while retries <= max_retries {
        let delay = Duration::from_secs(1 << retries.min(4))
            + Duration::from_millis(rand::random::<u64>() % 1000);
        let result = timeout(Duration::from_secs(60), client.get(url.clone()).send()).await;

        match result {
            Ok(Ok(response)) => match response.text().await {
                Ok(body) => return Some(body),
                Err(e) => eprintln!("Failed to read body for {}. Error: {}", url, e),
            },
            Ok(Err(e)) => {
                eprintln!("Failed to get URL: {}. Error: {}", url, e);
            }
            Err(_) => {
                eprintln!("Request to {} timed out.", url);
            }
        }

        retries += 1;
        eprintln!("Retrying {} in {}s...", url, delay.as_secs());
        time::sleep(delay).await;
    }

    eprintln!("Max retries reached for {}", url);
    None
}

fn sanitize_url(url: &Url) -> Option<Url> {
    let mut url = url.clone();

    url.set_fragment(None);

    let path = url.path();
    let encoded_path =
        percent_encoding::utf8_percent_encode(path, percent_encoding::NON_ALPHANUMERIC).to_string();
    url.set_path(&encoded_path);

    url.domain()?;

    Some(url)
}

async fn build_graph(
    urls: &[Url],
    semaphore: Arc<Semaphore>,
    path: &str,
    client: &Client,
) -> IoResult<()> {
    let total = urls.len();
    let progress = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel::<String>(2048);

    let path = path.to_string();
    let writer_handle = tokio::spawn(async move {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .expect("Failed to open output file");

        while let Some(line) = rx.recv().await {
            if let Err(e) = file.write_all(line.as_bytes()).await {
                eprintln!("Error writing to file: {}", e);
            }
            if let Err(e) = file.write_all(b"\n").await {
                eprintln!("Error writing newline: {}", e);
            }
        }
    });

    let stream = stream::iter(urls);

    stream
        .for_each_concurrent(50, |url| {
            let semaphore = semaphore.clone();
            let progress = Arc::clone(&progress);
            let tx = tx.clone();

            async move {
                let _permit = semaphore.acquire_owned().await.unwrap();

                if let Some(clean_url) = sanitize_url(url) {
                    if let Some(body) = fetch_url(&clean_url, 8, client).await {
                        let links = get_links(&clean_url, body);

                        let graph_entry = json!({
                            "url": clean_url.to_string(),
                            "links": links.into_iter().map(|link| link.to_string()).collect::<Vec<_>>(),
                        });

                        let graph_entry_str = serde_json::to_string(&graph_entry).unwrap();

                        if let Err(e) = tx.send(graph_entry_str).await {
                            eprintln!("Failed to send graph entry: {}", e);
                        }
                    }
                }

                let done = progress.fetch_add(1, Ordering::SeqCst) + 1;
                update_loading_bar(total, done, "Building Link Graph");
            }
        })
        .await;

    drop(tx);
    writer_handle.await.unwrap();

    Ok(())
}

fn build_reverse_graph(graph: &HashMap<Url, Vec<Url>>) -> HashMap<Url, Vec<Url>> {
    let mut reverse_graph: HashMap<Url, Vec<Url>> = HashMap::new();

    for (node, links) in graph {
        reverse_graph.entry(node.clone()).or_default();
        for link in links {
            reverse_graph
                .entry(link.clone())
                .or_default()
                .push(node.clone());
        }
    }

    reverse_graph
}

async fn load_graph(path: &str) -> IoResult<HashMap<Url, Vec<Url>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut graph: HashMap<Url, Vec<Url>> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let entry: serde_json::Value = serde_json::from_str(&line).unwrap();
        let url = entry["url"].as_str().unwrap().to_string();
        let links = entry["links"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|link| link.as_str().and_then(|s| Url::parse(s).ok()))
            .collect();

        if let Ok(url) = Url::parse(&url) {
            if let Some(clean) = sanitize_url(&url) {
                graph.insert(clean, links);
            }
        }
    }

    Ok(graph)
}

fn pagerank(graph: &HashMap<Url, Vec<Url>>) -> HashMap<Url, f64> {
    let damping_factor = 0.9;
    let max_iterations = 100;
    let tolerance = 1e-6;

    let reverse_graph = build_reverse_graph(graph);
    let num_nodes = reverse_graph.len() as f64;

    let mut pagerank: HashMap<Url, f64> = reverse_graph
        .keys()
        .map(|url| (url.clone(), 1.0 / num_nodes))
        .collect();

    let new_pagerank = Arc::new(Mutex::new(pagerank.clone()));

    let dangling_nodes: Vec<&Url> = reverse_graph
        .iter()
        .filter(|(_, incoming)| incoming.is_empty())
        .map(|(url, _)| url)
        .collect();

    let mut converged = false;

    for i in 0..max_iterations {
        if !converged {
            let dangling_sum: f64 = damping_factor
                * dangling_nodes
                    .par_iter()
                    .map(|node| pagerank[node])
                    .sum::<f64>()
                / num_nodes;

            let new_ranks: HashMap<Url, f64> = reverse_graph
                .par_iter()
                .map(|(url, incoming_links)| {
                    let mut rank = (1.0 - damping_factor) / num_nodes + dangling_sum;
                    rank += incoming_links
                        .iter()
                        .map(|incoming| {
                            damping_factor * pagerank[incoming] / graph[incoming].len() as f64
                        })
                        .sum::<f64>();
                    (url.clone(), rank)
                })
                .collect();

            let error: f64 = new_ranks
                .par_iter()
                .map(|(url, &rank)| (pagerank[url] - rank).abs())
                .sum();

            pagerank = new_ranks;

            if error < tolerance {
                converged = true;
            }
        }

        update_loading_bar(max_iterations, i + 1, "Computing Pagerank Scores");
    }

    pagerank
}

async fn index_helper(urls: &[Url], semaphore: Arc<Semaphore>, client: &Client) -> IoResult<()> {
    let semaphore_size = 50;
    let max_retries = 9;

    let word_list = Arc::new(load_word_list("wordlist.txt"));
    let stop_word_list = Arc::new(load_stop_word_list("stopwords.txt"));

    let total = urls.len();
    let progress = Arc::new(AtomicUsize::new(0));

    let (tx, mut rx) = mpsc::channel::<String>(2048);

    let writer_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("websites.jsonl")
            .await
            .expect("Failed to open output file");

        while let Some(line) = rx.recv().await {
            if let Err(e) = file.write_all(line.as_bytes()).await {
                eprintln!("Error writing to file: {}", e);
            }
            if let Err(e) = file.write_all(b"\n").await {
                eprintln!("Error writing newline: {}", e);
            }
        }
    });

    let retry_queue = Arc::new(Mutex::new(Vec::<(usize, Url, usize)>::new()));

    let stream = stream::iter(urls.iter().enumerate());

    stream
        .for_each_concurrent(semaphore_size, |(doc_id, url)| {
            let semaphore = semaphore.clone();
            let word_list = word_list.clone();
            let stop_word_list = stop_word_list.clone();
            let progress = progress.clone();
            let tx = tx.clone();
            let retry_queue = retry_queue.clone();
            let client = client.clone();

            async move {
                let _permit = semaphore.acquire_owned().await.unwrap();

                let jitter = rand::random::<u64>() % 3 + 1;
                time::sleep(Duration::from_secs(jitter)).await;

                let fetch_result = timeout(
                    Duration::from_secs(30),
                    fetch_website_data(doc_id, url, &word_list, &stop_word_list, &client),
                )
                .await;

                match fetch_result {
                    Ok(Ok(website)) => {
                        let json = serde_json::to_string(&website).unwrap();
                        if let Err(e) = tx.send(json).await {
                            eprintln!("Failed to send to writer task: {}", e);
                        }
                    }
                    Ok(Err(_)) | Err(_) => {
                        retry_queue.lock().await.push((doc_id, url.clone(), 1));
                    }
                }

                let done = progress.fetch_add(1, Ordering::SeqCst) + 1;
                update_loading_bar(total, done, "Collecting Websites");
            }
        })
        .await;

    for retry_round in 0..max_retries {
        let mut retry_guard = retry_queue.lock().await;
        if retry_guard.is_empty() {
            break;
        }

        let current_retries_raw = std::mem::take(&mut *retry_guard);
        drop(retry_guard);

        let mut seen = HashSet::new();
        let current_retries: Vec<_> = current_retries_raw
            .into_iter()
            .filter(|(_, url, _)| seen.insert(url.clone()))
            .collect();

        println!("\nRetrying {} failed pages...", current_retries.len());

        let retry_total = current_retries.len();
        let retry_progress = Arc::new(AtomicUsize::new(0));
        let retry_stream = stream::iter(current_retries);

        retry_stream
            .for_each_concurrent(semaphore_size, |(doc_id, url, attempt)| {
                let tx = tx.clone();
                let word_list = word_list.clone();
                let stop_word_list = stop_word_list.clone();
                let retry_queue = retry_queue.clone();
                let retry_progress = retry_progress.clone();
                let client = client.clone();

                async move {
                    let delay = Duration::from_secs(1 << attempt.min(3));
                    time::sleep(delay).await;

                    let fetch_result = timeout(
                        Duration::from_secs(60),
                        fetch_website_data(doc_id, &url, &word_list, &stop_word_list, &client),
                    )
                    .await;

                    match fetch_result {
                        Ok(Ok(website)) => {
                            let json = serde_json::to_string(&website).unwrap();
                            if let Err(e) = tx.send(json).await {
                                eprintln!("Failed to send retry result: {}", e);
                            }
                        }
                        Ok(Err(_)) | Err(_) => {
                            if attempt + 1 < max_retries {
                                retry_queue
                                    .lock()
                                    .await
                                    .push((doc_id, url.clone(), attempt + 1));
                            } else {
                                eprintln!("Permanently failed: {}", url);
                            }
                        }
                    }

                    let done = retry_progress.fetch_add(1, Ordering::SeqCst) + 1;
                    update_loading_bar(
                        retry_total,
                        done,
                        &format!("Retry Pass {}", retry_round + 1),
                    );
                }
            })
            .await;
    }

    drop(tx);
    writer_handle.await.unwrap();

    let final_failed = retry_queue.lock().await;
    if !final_failed.is_empty() {
        let mut fail_log = std::fs::File::create("permanent_failures.txt")?;
        for (_, url, _) in final_failed.iter() {
            writeln!(fail_log, "{}", url)?;
        }
        println!(
            "Logged {} permanently failed URLs to permanent_failures.txt.",
            final_failed.len()
        );
    }

    Ok(())
}

pub async fn index(urls: Vec<Url>, semaphore: Arc<Semaphore>) -> Result<(), Box<dyn Error>> {
    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to build reqwest client"),
    );
    println!();

    index_helper(&urls, semaphore.clone(), &client).await?;
    println!("Done collecting websites!");

    build_graph(&urls, semaphore, "graph.jsonl", &client).await?;
    println!("Done building graph!");

    // pray that the graph is small enough to fit in ram
    // PRAY!!!!!!
    let graph = load_graph("graph.jsonl").await?;

    let pageranks = pagerank(&graph);
    drop(graph);

    let normalized: HashMap<Url, f64> = {
        let scaled: Vec<_> = pageranks
            .iter()
            .map(|(url, &s)| (url.clone(), s.ln()))
            .collect();
        let (min, max) = scaled
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), (_, v)| {
                (min.min(*v), max.max(*v))
            });
        scaled
            .into_iter()
            .map(|(url, v)| (url, (v - min) / (max - min)))
            .collect()
    };

    drop(pageranks);

    println!("Done computing pagerank!");

    println!("Saving pageranks to CSV...");
    save_metadata("websites.jsonl", &normalized, "websites.csv")?;
    println!("Done saving pageranks!");

    drop(normalized);

    let inverted_index = build_inverted_index("websites.jsonl")?;

    println!("Saving inverted index to CSV...");
    save_inverted_index(&inverted_index, "inverted_index.csv")?;
    println!("Done saving inverted index!");

    drop(inverted_index);

    remove_file("websites.jsonl").await?;
    remove_file("graph.jsonl").await?;

    println!("Data successfully saved!");

    Ok(())
}
