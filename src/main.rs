use chrono::{DateTime, Local};
use clap::Parser;
use influxdb::{InfluxDbWriteable, ReadQuery};
use reqwest::{Client, Url};
mod cli;
mod models;

use crate::cli::Cli;
use crate::models::{Consumption, ConsumptionOrTariff, EnergyType, RequestType, Tariff};
const N3RGY_BASE_URL: &str = "https://consumer-api.data.n3rgy.com/";

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let influx_client =
        influxdb::Client::new(cli.influx_uri, cli.influx_database).with_token(cli.influx_token);

    let measurements = pull_usage(
        client,
        cli.start_date,
        cli.end_date,
        cli.energy_type,
        cli.request_type,
        cli.api_token,
    )
    .await
    .unwrap();

    let readings = construct_influx_measurements(measurements);

    influx_client.query(readings).await.unwrap();
}

fn construct_influx_measurements(
    parsed_messages: ConsumptionOrTariff,
) -> Vec<influxdb::WriteQuery> {
    let mut readings = Vec::new();
    if let ConsumptionOrTariff::Consumption(consumption) = parsed_messages {
        for m in consumption.influx_format() {
            readings.push(m.into_query("energy"));
        }
    } else if let ConsumptionOrTariff::Tariff(tariff) = parsed_messages {
        for m in tariff.influx_format() {
            readings.push(m.into_query("energy"));
        }
    }
    readings
}

async fn pull_usage(
    client: Client,
    start_date: DateTime<Local>,
    end_date: DateTime<Local>,
    energy_type: EnergyType,
    request_type: RequestType,
    api_token: String,
) -> Result<ConsumptionOrTariff, serde_json::Error> {
    let request_start = format!("{}", start_date.format("%Y%m%d%H%M"));
    let request_end = format!("{}", end_date.format("%Y%m%d%H%M"));

    let url = build_request_url(
        request_start,
        request_end,
        "JSON".to_string(),
        energy_type,
        request_type.clone(),
    );

    let res = client
        .get(url)
        .header("Authorization", api_token)
        .send()
        .await
        .unwrap();

    let body = res.text().await.unwrap();

    let measurement: ConsumptionOrTariff = match request_type {
        RequestType::Consumption => {
            ConsumptionOrTariff::Consumption(serde_json::from_str::<Consumption>(&body)?)
        }
        RequestType::Tariff => ConsumptionOrTariff::Tariff(serde_json::from_str::<Tariff>(&body)?),
    };

    Ok(measurement)
}

fn build_request_url(
    start: String,
    end: String,
    output: String,
    energy_type: EnergyType,
    request_type: RequestType,
) -> Url {
    let parameters = [("start", start), ("end", end), ("output", output)];

    let request_url = match energy_type {
        EnergyType::Electricity => N3RGY_BASE_URL.to_owned() + "electricity/",
        EnergyType::Gas => N3RGY_BASE_URL.to_owned() + "gas/",
    };

    let request_url = match request_type {
        RequestType::Consumption => request_url + "consumption/1",
        RequestType::Tariff => request_url + "tariff/1",
    };

    reqwest::Url::parse_with_params(&request_url, parameters).unwrap()
}
