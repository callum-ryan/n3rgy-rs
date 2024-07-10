use std::borrow::Borrow;

use chrono::{DateTime, Duration, Local, TimeZone};
use clap::Parser;
use influxdb::InfluxDbWriteable;
use reqwest::{Client, Url};
mod cli;
mod models;

use crate::cli::Cli;
use crate::models::{ConsumptionOrTariff, EnergyType, RequestType};
const N3RGY_BASE_URL: &str = "https://consumer-api.data.n3rgy.com/";

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let api_token: &str = cli.api_token.borrow();

    let client = reqwest::Client::new();
    let influx_client =
        influxdb::Client::new(cli.influx_uri, cli.influx_database).with_token(cli.influx_token);

    let date_difference = (cli.end_date - cli.start_date).num_days();

    if date_difference > 90 {
        let mut start_date = cli.start_date;
        let mut end_date = start_date + Duration::days(90);
        let mut date_batches = Vec::new();

        date_batches.push((start_date, end_date));

        while cli.end_date > end_date {
            start_date = start_date + Duration::days(90);
            end_date = min_dates(start_date + Duration::days(90), cli.end_date);
            date_batches.push((start_date, end_date));
        }
        for batch in date_batches {
            pull_and_load(
                &client,
                api_token,
                &influx_client,
                batch.0,
                batch.1,
                cli.energy_type,
                cli.request_type,
            )
            .await;
        }
    } else {
        pull_and_load(
            &client,
            api_token,
            &influx_client,
            cli.start_date,
            cli.end_date,
            cli.energy_type,
            cli.request_type,
        )
        .await;
    }
}

fn min_dates<Tz: TimeZone>(d1: DateTime<Tz>, d2: DateTime<Tz>) -> DateTime<Tz> {
    let d1_unix = d1.timestamp();
    let d2_unix = d2.timestamp();
    if d1_unix < d2_unix {
        return d1;
    } else if d1_unix > d2_unix {
        return d2;
    } else {
        return d1;
    }
}

async fn pull_and_load(
    api_client: &reqwest::Client,
    api_token: &str,
    influx_client: &influxdb::Client,
    start: DateTime<Local>,
    end: DateTime<Local>,
    energy_type: EnergyType,
    request_type: RequestType,
) {
    let measurements = pull_usage(api_client, start, end, energy_type, request_type, api_token)
        .await
        .unwrap();

    let readings = match measurements {
        ConsumptionOrTariff::Error(_) => construct_influx_measurements(measurements),
        ConsumptionOrTariff::Consumption(_) => construct_influx_measurements(measurements),
        ConsumptionOrTariff::Tariff(_) => construct_influx_measurements(measurements),
    };

    if readings.len() > 0 {
        influx_client.query(readings).await.unwrap();
    }
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
    } else if let ConsumptionOrTariff::Error(error) = parsed_messages {
        error.log_out();
    }
    readings
}

async fn pull_usage(
    client: &Client,
    start_date: DateTime<Local>,
    end_date: DateTime<Local>,
    energy_type: EnergyType,
    request_type: RequestType,
    api_token: &str,
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
    let measurement: ConsumptionOrTariff = serde_json::from_str(&body).unwrap();
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
