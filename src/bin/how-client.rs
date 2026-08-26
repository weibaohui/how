//! HOW client binary.

#[tokio::main]
async fn main() {
    // Load configuration (Go-style `-config` flag, default config.client.example.cfg).
    let config_file = how::cli::string_flag("config", "config.client.example.cfg");

    let config = match how::client::load_configuration(&config_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Unable to load configuration : {}", e);
            std::process::exit(1);
        }
    };

    // Dump the configuration as indented JSON.
    match serde_json::to_string_pretty(&config) {
        Ok(s) => println!("{}", s),
        Err(e) => eprintln!("Unable to dump config : {}", e),
    }

    let mut client = how::client::Client::new(config);

    // Handle SIGINT.
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        how::log::log("SIGINT Detected");
        std::process::exit(0);
    });

    client.start();

    // Block forever.
    std::future::pending::<()>().await;
}
