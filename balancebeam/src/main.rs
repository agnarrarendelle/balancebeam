mod request;
mod response;

use clap::Parser;
use rand::{Rng, SeedableRng};
use std::{
    collections::HashSet,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread, time::Duration,
};
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
    live_servers: Arc<Mutex<Vec<String>>>,

    live_servers_set: Arc<Mutex<HashSet<String>>>,
}

fn main() {
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
    let listener = match TcpListener::bind(&options.bind) {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let live_servers = Arc::new(Mutex::new(options.upstream.clone()));
    let live_servers_set: Arc<Mutex<HashSet<String>>> =
        Arc::new(Mutex::new(options.upstream.clone().into_iter().collect()));

    let mut state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        live_servers,
        live_servers_set,
    };
    let active_health_check_path = state.active_health_check_path.clone();
    let upstream_addresses = state.upstream_addresses.clone();

    let live_servers = state.live_servers.clone();
    let live_servers_set = state.live_servers_set.clone();
    let sleep_time = state.active_health_check_interval;

    thread::spawn(move || {
        let active_health_check_path = active_health_check_path;
        
        loop {
            thread::sleep(Duration::from_secs(sleep_time as u64));

            let mut live_servers = live_servers.lock().unwrap();
            live_servers.clear();
            // let mut live_servers_set = live_servers_set.lock().unwrap();

            
            for addr in &upstream_addresses {
                let request = http::Request::builder()
                    .method(http::Method::GET)
                    .uri(&active_health_check_path)
                    .header("Host", addr)
                    .body(vec![])
                    .unwrap();

                match TcpStream::connect(addr) {
                    Err(e) => {
                        print!("Failed to connect to server at {addr} because {e}");
                        continue;
                    }
                    Ok(mut stream) => {
                        if let Err(e) = request::write_to_stream(&request, &mut stream) {
                            println!("Faild to send content to server at {addr} because {e}");
                            continue;
                        }

                        let response = match response::read_from_stream(
                            &mut stream,
                            request.method(),
                        ) {
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
    });
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            // Handle the connection!
            handle_connection(stream, &mut state);
        }
    }
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

fn connect_to_upstream(upstream_ip: &str) -> Result<TcpStream, std::io::Error> {
    TcpStream::connect(upstream_ip).or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(err)
    })
    // TODO: implement failover (milestone 3)
}

fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn) {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

fn handle_connection(mut client_conn: TcpStream, state: &mut ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    let mut upstream_conn;
    loop {
        let live_addresses = state.live_servers.lock().unwrap();
        let mut live_addresses_set = state.live_servers_set.lock().unwrap();

        let random_index = get_random_index(&live_addresses);
        let random_upstream_addr = get_upstream_addr(&live_addresses, random_index);
        drop(live_addresses);
        drop(live_addresses_set);
        match connect_to_upstream(&random_upstream_addr) {
            Ok(stream) => {
                upstream_conn = stream;
                break;
            }
            Err(_) => {
                let mut live_addresses = state.live_servers.lock().unwrap();
                let mut live_addresses_set = state.live_servers_set.lock().unwrap();
                live_addresses_set.remove(&random_upstream_addr);
                live_addresses.swap_remove(random_index);

                if live_addresses.len() == 0 {
                    log::error!("No upstream is available");
                    let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                    send_response(&mut client_conn, &response);
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
        let mut request = match request::read_from_stream(&mut client_conn) {
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
                send_response(&mut client_conn, &response);
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
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn) {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()) {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response);
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response);
        log::debug!("Forwarded response to client");
    }
}
