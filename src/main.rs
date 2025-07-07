#![allow(unused)]

mod crawler;
mod indexer;

use crawler::crawl_and_collect;
use indexer::index;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

// currently using max depth of 3 with 3 starting links
async fn crawl_websites() -> std::io::Result<()> {
    async fn collect_links_verbose(
        pages: Vec<Url>,
        semaphore: Arc<Semaphore>,
        max: u8,
    ) -> HashSet<Url> {
        let mut all_links = HashSet::new();

        for page in pages {
            println!("Starting to crawl: {}", page);
            let links = crawl_and_collect(vec![page.clone()], max, semaphore.clone(), false).await;
            all_links.extend(links.clone());
            println!(
                "Finished crawling! Number of links found on page: {}",
                links.len()
            );
        }

        all_links
    }

    async fn collect_links(pages: Vec<Url>, semaphore: Arc<Semaphore>, max: u8) -> HashSet<Url> {
        crawl_and_collect(pages, max, semaphore, false).await
    }

    fn display_links(links: HashSet<Url>) {
        println!("-----------------------");
        println!("Unique Links Visited:");
        println!("-----------------------");
        let links: Vec<_> = links.into_iter().collect();
        for link in links.clone() {
            println!("{}", link);
        }
    }

    async fn save_unique_links(links: Vec<Url>) -> std::io::Result<()> {
        let file_path = "links.txt";
        let mut existing_links = HashSet::new();

        if Path::new(file_path).exists() {
            let file = File::open(file_path)?;
            let reader = BufReader::new(file);

            reader.lines().map_while(Result::ok).for_each(|link| {
                existing_links.insert(link);
            });
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;

        for link in links {
            let link_str = link.as_str();
            if !existing_links.contains(link_str) {
                writeln!(file, "{}", link_str)?;
            }
        }

        Ok(())
    }

    println!("Starting to crawl website(s)");
    println!("This might take a while...");

    let depth = 3;

    // 3 starting links
    let pages = [
        "https://en.wikipedia.org/wiki/List_of_sovereign_states",
		"https://stackoverflow.com/questions",
        "https://www.geeksforgeeks.org",
    ]
    .into_iter()
    .map(|link| Url::parse(link).unwrap())
    .collect();

    let semaphore = Arc::new(Semaphore::new(250));

    let links = collect_links(pages, semaphore, depth).await;

    if !links.is_empty() {
        print!("\rUnique Links Found: {}  ", links.len());
        io::stdout().flush().unwrap();
        //display_links();
        let links: Vec<_> = links.into_iter().collect();
        save_unique_links(links).await?;
    } else {
        println!("No Unique Links Found For Webpage");
    }

    Ok(())
}

async fn index_websites() {
    let semaphore = Arc::new(Semaphore::new(500));
    let contents = fs::read_to_string("links.txt").unwrap();
    let urls = contents
        .lines()
        .filter_map(|line| Url::parse(line).ok())
        .collect();

    index(urls, semaphore).await;
}

#[tokio::main]
async fn main() {
    crawl_websites().await;
    index_websites().await;
}
