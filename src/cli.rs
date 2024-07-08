use chrono::{DateTime, Local};
use clap::{builder::TypedValueParser, Parser};

use crate::models::{EnergyType, RequestType};

#[derive(Parser)]
#[command(about = "Pull data from n3rgy API")]
pub struct Cli {
    #[arg(value_parser = clap::builder::StringValueParser::new().try_map(parse_dt))]
    pub start_date: DateTime<Local>,
    #[arg(value_parser = clap::builder::StringValueParser::new().try_map(parse_dt))]
    pub end_date: DateTime<Local>,
    pub energy_type: EnergyType,
    pub request_type: RequestType,
    #[clap(env)]
    pub api_token: String,
    #[clap(env)]
    pub influx_uri: String,
    #[clap(env)]
    pub influx_database: String,
    #[clap(env)]
    pub influx_token: String,
}

fn parse_dt(value: String) -> Result<chrono::DateTime<Local>, chrono::ParseError> {
    if let Ok(dt) = value.parse::<chrono::DateTime<Local>>() {
        Ok(dt)
    } else {
        let naive_date = value.parse::<chrono::NaiveDate>().unwrap();
        Ok(naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap())
    }
}
