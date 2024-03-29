mod request;
mod response;

use clap::Parser;
use rand::{Rng, SeedableRng};
use std::{collections::HashMap, net::IpAddr, sync::Arc, thread, time::Duration};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::delay_for;
/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing")]
struct CmdOptions {
    /// "IP/port to bind to"
    #[arg(short, long, default_value = "0.0.0.0:1100")]
    bind: String,
    /// "Upstream host to forward requests to"
    #[arg(short, long)]
    upstream: Vec<String>,
    /// "Perform active health checks on this interval (in seconds)"
    #[arg(long, default_value = "10")]
    active_health_check_interval: usize,
    /// "Path to send request to for active health checks"
    #[arg(long, default_value = "/")]
    active_health_check_path: String,
    /// "Maximum number of requests to accept per IP per minute (0 = unlimited)"
    #[arg(long, default_value = "0")]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
#[derive(Clone)]
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,

    // Servers that are live
    live_servers: Arc<RwLock<Vec<String>>>,

    rate_limiting_counters: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

#[tokio::main]
async fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let mut listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let live_servers = Arc::new(RwLock::new(options.upstream.clone()));

    let state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        live_servers,
        rate_limiting_counters: Arc::new(Mutex::new(HashMap::new())),
    };

    if state.max_requests_per_minute > 0 {
        let counters = state.rate_limiting_counters.clone();
        tokio::spawn(async move { refresh_rate_limiting_counter(counters, 60).await });
    }

    let cloned_state = state.clone();
    tokio::spawn(async move { active_health_check(cloned_state).await });

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            if state.max_requests_per_minute > 0 {
                let mut counters = state.rate_limiting_counters.lock().await;
                let ip_addr = stream.peer_addr().unwrap().ip();
                let counter = counters.entry(ip_addr).or_insert(0);
                *counter += 1;

                if *counter > state.max_requests_per_minute {
                    let res = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
                    response::write_to_stream(&res, &mut stream).await.unwrap();
                    continue;
                }
            }

            // Handle the connection!
            let mut state = state.clone();
            tokio::spawn(async move {
                handle_connection(stream, &mut state).await;
            });
        }
    }
}

async fn active_health_check(state: ProxyState) {
    let active_health_check_path = state.active_health_check_path.clone();

    loop {
        delay_for(Duration::from_secs(
            state.active_health_check_interval as u64,
        ))
        .await;

        let mut live_servers = state.live_servers.write().await;
        live_servers.clear();

        for addr in &state.upstream_addresses {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(&active_health_check_path)
                .header("Host", addr)
                .body(vec![])
                .unwrap();

            match TcpStream::connect(addr).await {
                Err(e) => {
                    print!("Failed to connect to server at {addr} because {e}");
                    continue;
                }
                Ok(mut stream) => {
                    if let Err(e) = request::write_to_stream(&request, &mut stream).await {
                        println!("Faild to send content to server at {addr} because {e}");
                        continue;
                    }

                    let response =
                        match response::read_from_stream(&mut stream, request.method()).await {
                            Ok(response) => response,
                            Err(e) => {
                                log::error!(
                                    "Failed to read response from server at {addr} because {:?}",
                                    e
                                );
                                continue;
                            }
                        };

                    match response.status().as_u16() {
                        200 => {
                            live_servers.push(addr.clone());
                        }
                        _ => {
                            log::error!("server at {addr} has some problem");

                            continue;
                        }
                    }
                }
            }
        }
    }
}

async fn refresh_rate_limiting_counter(
    counters: Arc<Mutex<HashMap<IpAddr, usize>>>,
    interval: u64,
) {
    thread::sleep(Duration::from_secs(interval));
    let mut counters = counters.lock().await;
    counters.clear();
}

fn get_random_index<T>(addrs: &Vec<T>) -> usize {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let random_index = rng.gen_range(0, addrs.len());
    random_index
}

fn get_upstream_addr(addrs: &Vec<String>, index: usize) -> String {
    let upstream_ip = &addrs[index];
    upstream_ip.to_string()
}

async fn connect_to_upstream(upstream_ip: &str) -> Result<TcpStream, std::io::Error> {
    TcpStream::connect(upstream_ip).await.or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(err)
    })
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

async fn handle_connection(mut client_conn: TcpStream, state: &mut ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    let mut upstream_conn;
    loop {
        let live_addresses = state.live_servers.read().await;

        let random_index = get_random_index(&live_addresses);
        let random_upstream_addr = get_upstream_addr(&live_addresses, random_index);
        drop(live_addresses);
        match connect_to_upstream(&random_upstream_addr).await {
            Ok(stream) => {
                upstream_conn = stream;
                break;
            }
            Err(_) => {
                let mut live_addresses = state.live_servers.write().await;
                live_addresses.swap_remove(random_index);

                if live_addresses.len() == 0 {
                    log::error!("No upstream is available");
                    let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                    send_response(&mut client_conn, &response).await;
                    return;
                }
            }
        }
    }

    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()).await
        {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
