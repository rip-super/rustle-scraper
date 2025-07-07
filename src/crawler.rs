#![allow(unused)]

use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, TagToken, Token, TokenSink, TokenSinkResult, Tokenizer,
    TokenizerOpts,
};

use robotxt::Robots;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time;
use url::{ParseError, Url};

#[derive(Clone, Default)]
pub struct RobotsCache {
    inner: Arc<Mutex<HashMap<String, Robots>>>,
    client: reqwest::Client,
}

impl RobotsCache {
    fn new(client: reqwest::Client) -> RobotsCache {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            client,
        }
    }

    pub async fn can_crawl(&self, url: &Url, ua: &str) -> bool {
        let domain = match url.domain() {
            Some(d) => d.to_string(),
            None => return false,
        };

        {
            let guard = self.inner.lock().await;
            if let Some(r) = guard.get(&domain) {
                return r.is_relative_allowed(url.path());
            }
        }

        let robots_url = format!("{}://{}/robots.txt", url.scheme(), domain);
        let res = match self.client.get(&robots_url).send().await {
            Ok(r) => r,
            Err(_) => return false,
        };

        let bytes = match res.bytes().await {
            Ok(b) => b,
            Err(_) => return false,
        };

        let r = Robots::from_bytes(&bytes, ua);
        {
            let mut guard = self.inner.lock().await;
            guard.insert(domain.clone(), r.clone());
        }

        r.is_relative_allowed(url.path())
    }
}

type CrawlResult = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
type BoxFuture = std::pin::Pin<Box<dyn std::future::Future<Output = CrawlResult> + Send>>;

#[derive(Default, Debug)]
struct LinkQueue {
    links: Vec<String>,
}

impl TokenSink for &mut LinkQueue {
    type Handle = ();

    fn process_token(&mut self, token: Token, line_number: u64) -> TokenSinkResult<Self::Handle> {
        if let TagToken(
            ref tag @ Tag {
                kind: TagKind::StartTag,
                ..
            },
        ) = token
        {
            if tag.name.as_ref() == "a" {
                for attr in tag.attrs.iter() {
                    if attr.name.local.as_ref() == "href" {
                        let url_str: &[u8] = attr.value.borrow();
                        self.links
                            .push(String::from_utf8_lossy(url_str).into_owned());
                    }
                }
            }
        }

        TokenSinkResult::Continue
    }
}

pub fn get_links(url: &Url, page: String) -> Vec<Url> {
    let mut domain_url = url.clone();
    let domain_host = domain_url.host_str().map(|s| s.to_string());
    domain_url.set_path("");
    domain_url.set_query(None);

    let mut queue = LinkQueue::default();
    let mut tokenizer = Tokenizer::new(&mut queue, TokenizerOpts::default());
    let mut buffer = BufferQueue::new();
    buffer.push_back(page.into());
    let _ = tokenizer.feed(&mut buffer);

    let is_wikipedia = domain_host.as_deref() == Some("en.wikipedia.org");

    let non_article_prefixes = [
        "/wiki/Special:",
        "/wiki/Category:",
        "/wiki/Template:",
        "/wiki/File:",
        "/wiki/Help:",
        "/wiki/Portal:",
        "/wiki/Talk:",
        "/wiki/Template_talk:",
        "/wiki/User:",
        "/wiki/Wikipedia:",
    ];

    queue
        .links
        .iter()
        .filter_map(|link| {
            let parsed_url = match Url::parse(link) {
                Ok(url) => Some(url),
                Err(ParseError::RelativeUrlWithoutBase) => domain_url.join(link).ok(),
                Err(_) => None,
            };

            if let Some(ref url) = parsed_url {
                if url.fragment().is_some() {
                    return None;
                }
            }

            if is_wikipedia {
                if let Some(ref url) = parsed_url {
                    if !url.path().starts_with("/wiki/") {
                        return None;
                    }

                    if non_article_prefixes
                        .iter()
                        .any(|prefix| url.path().starts_with(prefix))
                    {
                        return None;
                    }

                    if url.host_str() == domain_host.as_deref() {
                        return Some(url.clone());
                    }
                }

                if parsed_url.as_ref().and_then(|u| u.host_str()) == domain_host.as_deref() {
                    println!("Malformed link found, skipping: {}", link);
                    return None;
                }

                None
            } else {
                parsed_url
            }
        })
        .collect()
}

#[derive(Clone)]
pub struct CrawlContext {
    unique_links: Arc<Mutex<HashSet<Url>>>,
    semaphore: Arc<Semaphore>,
    article_counter: Arc<AtomicUsize>,
    debug: bool,
    robots_cache: RobotsCache,
    user_agent: String,
}

pub fn crawl(pages: Vec<Url>, current: u8, max: u8, context: CrawlContext) -> BoxFuture {
    Box::pin(crawl_helper(pages, current, max, context))
}

fn normalize_url(mut url: Url) -> Url {
    if url.scheme() == "http" {
        url.set_scheme("https").ok();
    }

    if let Some(host) = url.host_str() {
        if host == "geeksforgeeks.org" {
            url.set_host(Some("www.geeksforgeeks.org")).ok();
        }
    }

    url
}

async fn crawl_helper(pages: Vec<Url>, current: u8, max: u8, context: CrawlContext) -> CrawlResult {
    if context.debug {
        println!("Current Depth: {}, Max Depth: {}", current, max);
    }

    if current >= max {
        if context.debug {
            println!("Reached Max Depth");
        }
        return Ok(());
    }

    let mut tasks = vec![];

    if context.debug {
        println!("Crawling: {:#?}", pages);
    }

    let client = Arc::new(
        reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to build reqwest client"),
    );

    for url in pages {
        let context_clone = context.clone();
        let unique_links_clone = context_clone.unique_links.clone();
        let semaphore_clone = context_clone.semaphore.clone();
        let article_counter_clone = context_clone.article_counter.clone();
        let robots_cache_clone = context_clone.robots_cache.clone();
        let user_agent_clone = context_clone.user_agent.clone();
        let client_clone = client.clone();

        {
            let mut set = unique_links_clone.lock().await;
            if !set.insert(url.clone()) {
                continue;
            }
        }

        let task = spawn(async move {
            let links = {
                let permit = match semaphore_clone.acquire().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                let _permit_guard = permit;

                let can_crawl = match tokio::time::timeout(
                    Duration::from_secs(60),
                    robots_cache_clone.can_crawl(&url, &user_agent_clone),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        eprintln!("Robots.txt check timed out for {}", url);
                        return;
                    }
                };

                if !can_crawl {
                    return;
                }

                let mut retries = 0;
                let max_retries = 5;

                let body_bytes = loop {
                    let response = tokio::time::timeout(
                        Duration::from_secs(60),
                        client_clone.get(url.clone()).send(),
                    )
                    .await;

                    let res = match response {
                        Ok(Ok(r)) => r,
                        Ok(Err(e)) => {
                            eprintln!("Failed to get URL: {}. Error: {}", url, e);
                            retries += 1;
                            if retries >= max_retries {
                                eprintln!("Max retries reached for URL: {}", url);
                                return;
                            }
                            let backoff = 2_u64.pow(retries as u32).min(30);
                            time::sleep(Duration::from_secs(backoff)).await;
                            continue;
                        }
                        Err(_) => {
                            eprintln!("Request timed out for URL: {}", url);
                            retries += 1;
                            if retries >= max_retries {
                                eprintln!("Max retries reached for URL: {}", url);
                                return;
                            }
                            let backoff = 2_u64.pow(retries as u32).min(30);
                            time::sleep(Duration::from_secs(backoff)).await;
                            continue;
                        }
                    };

                    match tokio::time::timeout(Duration::from_secs(60), res.bytes()).await {
                        Ok(Ok(bytes)) => break bytes,
                        Ok(Err(e)) => {
                            eprintln!("Failed to read body for {}. Error: {}", url, e);
                        }
                        Err(_) => {
                            eprintln!("Body read timed out for {}", url);
                        }
                    }

                    retries += 1;
                    if retries >= max_retries {
                        eprintln!("Max retries reached for body on URL: {}", url);
                        return;
                    }

                    time::sleep(Duration::from_secs(3)).await;
                };

                let body = match String::from_utf8(body_bytes.to_vec()) {
                    Ok(valid_body) => valid_body,
                    Err(e) => {
                        eprintln!("Failed to parse body as UTF-8 for {}. Error: {}", url, e);
                        return;
                    }
                };

                let count = article_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                print!("\rWebsites processed: {}", count);
                io::stdout().flush().unwrap();

                get_links(&url, body)
            };

            if let Err(e) = crawl(links, current + 1, max, context_clone.clone()).await {
                eprintln!("Crawl failed: {}", e);
            }
        });

        tasks.push(task);
    }

    for task in tasks.into_iter() {
        task.await;
    }

    Ok(())
}

pub async fn crawl_and_collect(
    pages: Vec<Url>,
    max: u8,
    semaphore: Arc<Semaphore>,
    debug: bool,
) -> HashSet<Url> {
    let unique_links = Arc::new(Mutex::new(HashSet::new()));
    let article_counter = Arc::new(AtomicUsize::new(0));
    let robots_cache = RobotsCache::new(reqwest::Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36").timeout(Duration::from_secs(60)).build().expect("Failed to build reqwest client"));
    let user_agent = "RustleBot/1.0".to_string();
    let context = CrawlContext {
        unique_links: unique_links.clone(),
        semaphore,
        article_counter,
        debug,
        robots_cache,
        user_agent,
    };
    let result = crawl(pages, 0, max, context).await;

    if let Err(e) = result {
        eprintln!("Crawl failed: {}", e);
    }

    let links = unique_links.lock().await;

    links.clone()
}
