use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use clap::ValueEnum;
use influxdb::InfluxDbWriteable;
use serde::Deserialize;

mod n3rgy_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{Deserialize, Deserializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}

#[derive(Copy, Clone, ValueEnum)]
pub enum EnergyType {
    Electricity,
    Gas,
}

#[derive(Copy, Clone, ValueEnum)]
pub enum RequestType {
    Consumption,
    Tariff,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConsumptionOrTariff {
    Consumption(Consumption),
    Tariff(Tariff),
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Consumption {
    resource: String,
    response_timestamp: String,
    start: String,
    end: String,
    granularity: String,
    values: Vec<Value>,
    message: Option<String>,
    unit: String,
}

impl Consumption {
    pub fn influx_format(&self) -> Vec<ConsumptionReading> {
        let mut readings = Vec::new();
        let values = self.values.clone();
        for value in values {
            readings.push(
                ConsumptionReading::new()
                    .consumption(value.value)
                    .time(value.timestamp)
                    .measurement(self.resource.clone())
                    .build(),
            );
        }
        readings
    }
}

#[derive(Default, Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Value {
    #[serde(with = "n3rgy_date_format")]
    timestamp: DateTime<Utc>,
    value: f64,
    status: Option<String>,
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tariff {
    resource: String,
    response_timestamp: String,
    start: String,
    end: String,
    values: Vec<TariffValues>,
}

impl Tariff {
    pub fn influx_format(&self) -> Vec<TariffPrice> {
        let mut readings = Vec::new();
        let values = self.values.clone();
        let resource = self.resource.clone();
        for value in values {
            for price in value.prices {
                readings.push(
                    TariffPrice::new()
                        .price(price.value)
                        .time(price.timestamp)
                        .price_type("Price".to_string())
                        .measurement(resource.clone())
                        .build(),
                );
            }
            for stdcharge in value.standing_charges {
                let start_time = stdcharge.start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
                readings.push(
                    TariffPrice::new()
                        .price(stdcharge.value)
                        .time(start_time)
                        .price_type("StandingCharge".to_string())
                        .measurement(resource.clone())
                        .build(),
                )
            }
        }

        readings
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TariffValues {
    standing_charges: Vec<StandingCharge>,
    prices: Vec<Price>,
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandingCharge {
    start_date: NaiveDate,
    value: f64,
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Price {
    #[serde(with = "n3rgy_date_format")]
    timestamp: DateTime<Utc>,
    value: f64,
}

#[derive(InfluxDbWriteable, Clone, Debug, Default)]
pub struct ConsumptionReading {
    time: DateTime<Utc>,
    consumption: f64,
    #[influxdb(tag)]
    measurement: String,
}

impl ConsumptionReading {
    fn new() -> ConsumptionReading {
        ConsumptionReading {
            time: Utc::now(),
            consumption: 0.0,
            measurement: "default".to_string(),
        }
    }

    fn time(&mut self, time: DateTime<Utc>) -> &mut ConsumptionReading {
        self.time = time;
        self
    }

    fn consumption(&mut self, consumption: f64) -> &mut ConsumptionReading {
        self.consumption = consumption;
        self
    }

    fn measurement(&mut self, measurement: String) -> &mut ConsumptionReading {
        self.measurement = measurement;
        self
    }

    fn build(&self) -> ConsumptionReading {
        ConsumptionReading {
            time: self.time,
            consumption: self.consumption,
            measurement: self.measurement.clone(),
        }
    }
}

#[derive(InfluxDbWriteable, Clone, Debug, Default)]
pub struct TariffPrice {
    time: DateTime<Utc>,
    price: f64,
    #[influxdb(tag)]
    measurement: String,
    #[influxdb(tag)]
    price_type: String,
}

impl TariffPrice {
    fn new() -> TariffPrice {
        TariffPrice {
            time: Utc::now(),
            price: 0.0,
            measurement: "default".to_string(),
            price_type: "default".to_string(),
        }
    }

    fn time(&mut self, time: DateTime<Utc>) -> &mut TariffPrice {
        self.time = time;
        self
    }

    fn price(&mut self, price: f64) -> &mut TariffPrice {
        self.price = price;
        self
    }

    fn measurement(&mut self, measurement: String) -> &mut TariffPrice {
        self.measurement = measurement;
        self
    }

    fn price_type(&mut self, price_type: String) -> &mut TariffPrice {
        self.price_type = price_type;
        self
    }

    fn build(&self) -> TariffPrice {
        TariffPrice {
            time: self.time,
            price: self.price,
            measurement: self.measurement.clone(),
            price_type: self.price_type.clone(),
        }
    }
}
