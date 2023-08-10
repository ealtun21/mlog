use chrono::{Datelike, Local, Timelike};
use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS, SubscribeReasonCode};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, self},
    io::{self, Write},
    time::Duration,
};

/// MQTT Logger
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The broker's domain name or IP address
    #[arg(short, long)]
    broker: String,

    /// The port number on which broker must be listening for incoming connections
    #[arg(short, long)]
    port: u16,

    /// The topics that the will be listened for
    #[arg(short, long, num_args(0..), required=true, conflicts_with("topics_file"))]
    topics: Vec<String>,

    /// Path to a file containing topics
    #[arg(short = 'f', long = "topics-file")]
    topics_file: Option<String>,

    /// The string to identify the device connecting to a broker
    #[arg(short, long, default_value = "mqtt-logger")]
    id: String,

    /// The number of seconds after which client should ping the broker if there is no other data exchange
    #[arg(short, long = "keep-alive", default_value_t = 5, value_name = "SEC")]
    keep_alive: u64,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let mut mqttoptions = MqttOptions::new(args.id, args.broker, args.port);
    mqttoptions.set_keep_alive(Duration::from_secs(args.keep_alive));

    let topics = if let Some(path) = args.topics_file {
        fs::read_to_string(&path)?
            .trim()
            .lines()
            .map(str::trim)
            .map(String::from)
            .collect()
    } else {
        args.topics
    };

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let mut files = HashMap::new();
    println!("Topics: {:?}", topics);
    for topic in topics {
        if client.subscribe(&topic, QoS::ExactlyOnce).await.is_err() {
            eprintln!("Failed to subscribe to {}", &topic);
        };
        files.insert(
            topic.clone(),
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{topic}.txt"))
                .expect("Unable to create files"),
        );
    }

    loop {
        match eventloop.poll().await {
            Ok(notification) => match notification {
                Event::Incoming(p) => match p {
                    Packet::Publish(p) => write(&p, &files),
                    Packet::SubAck(s) => {
                        for code in s.return_codes {
                            match code {
                                SubscribeReasonCode::Failure => {
                                    eprintln!("Got a subscribe fail packet!");
                                }
                                SubscribeReasonCode::Success(_) => (),
                            }
                        }
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

fn write(data: &Publish, files: &HashMap<String, File>) {
    let timestamp = generate_timestamp().into_bytes();
    let mut res = Vec::with_capacity(data.payload.len() + timestamp.len());
    res.extend_from_slice(&timestamp);
    res.extend_from_slice(&data.payload);

    
    match files.get(data.topic.as_str()) {
        Some(mut file) => file.write_all(&res).unwrap(),
        None => eprintln!(
            "Got packet from topic {}, but that topic file was not created!",
            data.topic
        ),
    };

    res.clear();
    res.extend_from_slice(&timestamp);
    res.extend_from_slice(format!("[{}] ",data.topic.as_str()).as_bytes());
    res.extend_from_slice(&data.payload);

    io::stdout().write_all(&res).unwrap();
}

fn generate_timestamp() -> String {
    let now = Local::now();
    format!(
        "[{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}] ",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis()
    )
}
