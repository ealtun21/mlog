use chrono::{Datelike, Local, Timelike};
use clap::Parser;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, MqttOptions, Packet, Publish, QoS, SubscribeReasonCode, EventLoop,
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    time::Duration,
};

/// MQTT Logger
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Domain name or IP address of the broker.
    #[arg(short, long)]
    broker: String,

    /// Port on which the broker is expected to listen for incoming connections.
    #[arg(short, long)]
    port: u16,

    /// Topics to be monitored.
    #[arg(short, long, num_args(1..), required=true, conflicts_with("topics_file"))]
    topics: Vec<String>,

    /// Path to a file that lists the topics.
    #[arg(short = 'f', long)]
    topics_file: Option<String>,

    /// Identifier for the device connecting to the broker.
    #[arg(short, long, default_value = "mqtt-logger")]
    id: String,

    /// Duration in seconds to wait before pinging the broker if there's no other communication.
    #[arg(short, long, default_value_t = 5, value_name = "SEC")]
    keep_alive: u64,

    /// Number of concurrent in flight messages
    #[arg(long)]
    inflight: Option<u16>,

    /// Credentials for logging in: username followed by password.
    #[arg(short, long, num_args(2))]
    auth: Vec<String>,

    /// Max packet size: incoming followed by outgoing.
    #[arg(short, long, num_args(2))]
    max_packet_size: Vec<usize>,

    /// Request channel capacity
    #[arg(short, long)]
    channel_capacity: Option<usize>,

    /// Clean Session
    #[arg(short, long)]
    clean_session: bool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let mqttoptions = configure_mqtt(&args);

    let topics = initialize_topics(&args)?;

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let mut files = initialize_files_and_subscriptions(&client, &topics).await;

    process_events(&mut eventloop, &mut files).await
}

fn configure_mqtt(args: &Args) -> MqttOptions {
    let mut mqttoptions = MqttOptions::new(&args.id, &args.broker, args.port);

    if !args.auth.is_empty() {
        mqttoptions.set_credentials(args.auth[0].clone(), args.auth[1].clone());
    }
    if let Some(inflight) = args.inflight {
        mqttoptions.set_inflight(inflight);
    }
    if !args.max_packet_size.is_empty() {
        mqttoptions.set_max_packet_size(args.max_packet_size[0], args.max_packet_size[1]);
    }
    if let Some(c_cap) = args.channel_capacity {
        mqttoptions.set_request_channel_capacity(c_cap);
    }
    mqttoptions.set_clean_session(args.clean_session);
    mqttoptions.set_keep_alive(Duration::from_secs(args.keep_alive));

    mqttoptions
}

fn initialize_topics(args: &Args) -> std::io::Result<Vec<String>> {
    if let Some(path) = &args.topics_file {
        Ok(fs::read_to_string(path)?
            .trim()
            .lines()
            .map(str::trim)
            .map(String::from)
            .collect())
    } else {
        Ok(args.topics.clone())
    }
}

async fn initialize_files_and_subscriptions(client: &AsyncClient, topics: &[String]) -> HashMap<String, File> {
    let mut files = HashMap::new();
    println!("Selected topics: {topics:?}");
    for topic in topics {
        if client.subscribe(topic, QoS::ExactlyOnce).await.is_err() {
            eprintln!("Failed to subscribe to {topic}");
        }
        files.insert(
            topic.clone(),
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{topic}.txt"))
                .expect("Unable to create files"),
        );
    }
    files
}

async fn process_events(eventloop: &mut EventLoop, files: &mut HashMap<String, File>) -> std::io::Result<()> {
    loop {
        match eventloop.poll().await {
            Ok(notification) => match notification {
                Event::Incoming(p) => match p {
                    Packet::Publish(p) => {
                        let timestamp = generate_timestamp().into_bytes();
                        write_to_file(&timestamp, &p, files);
                        write_to_stdout(&timestamp, &p);
                    }
                    Packet::SubAck(s) => {
                        for code in s.return_codes {
                            if code == SubscribeReasonCode::Failure {
                                eprintln!("Got a subscribe fail packet!");
                            }
                        }
                    }
                    Packet::ConnAck(c) if c.code == ConnectReturnCode::Success => {
                        println!("Connection established");
                    }
                    Packet::Disconnect => println!("Got disconnect"),
                    _ => (),
                },
                Event::Outgoing(_) => (),
            },
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }

    Ok(())
}

fn write_to_file(timestamp: &Vec<u8>, data: &Publish, files: &HashMap<String, File>) {
    let mut res = Vec::with_capacity(data.payload.len() + timestamp.len());

    res.extend_from_slice(timestamp);
    res.extend_from_slice(&data.payload);
    res.extend_from_slice("\n".as_bytes());

    match files.get(data.topic.as_str()) {
        Some(mut file) => {
            file.write_all(&res).unwrap();
            file.flush().unwrap();
        }
        None => eprintln!(
            "Got packet from topic {}, but that topic file was not created!",
            data.topic
        ),
    };
}

fn write_to_stdout(timestamp: &Vec<u8>, data: &Publish) {
    let mut res = Vec::with_capacity(data.payload.len() + timestamp.len());

    res.extend_from_slice(timestamp);
    res.extend_from_slice(
        format!(
            "{RESET}[{BLUE}{}{RESET}] ",
            data.topic.as_str(),
            RESET = "\x1b[0m",
            BLUE = "\x1b[34m"
        )
        .as_bytes(),
    );
    res.extend_from_slice(&data.payload);
    res.extend_from_slice("\n".as_bytes());

    io::stdout().write_all(&res).unwrap();
    ::std::io::stdout().flush().unwrap();
}

fn generate_timestamp() -> String {
    let now = Local::now();

    format!(
        "{RESET}[{GREEN}{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}{RESET}] ",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis(),
        RESET = "\x1b[0m",
        GREEN = "\x1b[32m",
    )
}
