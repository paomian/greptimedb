// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::physical_plan::PhysicalPlanRef;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream};
use common_telemetry::metric::*;
use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::array::StringArray;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;
use prometheus_parse::Value;
use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::Result;
use crate::metadata::{TableId, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType};
use crate::table::scan::SimpleTableScan;
use crate::table::{Expr, Table};

pub const METRICS_TABLE_NAME: &str = "metrics";

pub struct MetricsTable {
    table_id: TableId,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl Table for MetricsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        Arc::new(
            TableInfoBuilder::default()
                .table_id(self.table_id)
                .name(METRICS_TABLE_NAME)
                .catalog_name(DEFAULT_CATALOG_NAME)
                .schema_name(DEFAULT_SCHEMA_NAME)
                .table_version(0)
                .table_type(TableType::Base)
                .meta(
                    TableMetaBuilder::default()
                        .schema(self.schema.clone())
                        .region_numbers(vec![0])
                        .primary_key_indices(vec![0])
                        .next_column_id(1)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
    }

    async fn scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let stream = Box::pin(MetricsStream {
            schema: self.schema.clone(),
            already_run: false,
        });
        Ok(Arc::new(SimpleTableScan::new(stream)))
    }
}

impl MetricsTable {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            schema: Arc::new(build_metrics_schema()),
        }
    }
}

impl Default for MetricsTable {
    fn default() -> Self {
        MetricsTable::new(1)
    }
}

struct MetricsStream {
    schema: SchemaRef,
    already_run: bool,
}

impl RecordBatchStream for MetricsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MetricsStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.already_run {
            return Poll::Ready(None);
        }
        self.already_run = true;
        let handle = try_handle().unwrap();
        let metric_text = handle.render();
        let lines = metric_text.lines().map(|s| Ok(s.to_owned()));
        let samples = prometheus_parse::Scrape::parse(lines)
            .unwrap()
            .samples
            .into_iter()
            .map(|s| {
                let kind = match s.value {
                    prometheus_parse::Value::Counter(_) => "counter",
                    prometheus_parse::Value::Gauge(_) => "gauge",
                    prometheus_parse::Value::Untyped(_) => "untyped",
                    prometheus_parse::Value::Histogram(_) => "histogram",
                    prometheus_parse::Value::Summary(_) => "summary",
                }
                .to_string();
                (s.metric, kind, s.labels, s.value)
            })
            .collect::<Vec<_>>();
        let mut metrics: Vec<String> = Vec::with_capacity(samples.len());
        let mut labels: Vec<String> = Vec::with_capacity(samples.len());
        let mut kinds: Vec<String> = Vec::with_capacity(samples.len());
        let mut values: Vec<String> = Vec::with_capacity(samples.len());
        for sample in samples.into_iter() {
            metrics.push(sample.0);
            kinds.push(sample.1);
            labels.push(generate_labels((*sample.2).to_owned()));
            values.push(generate_value(sample.3));
        }
        let batch = DfRecordBatch::try_new(
            // metric kind labels value
            self.schema.arrow_schema().clone(),
            vec![
                Arc::new(StringArray::from(metrics)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(StringArray::from(labels)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();

        Poll::Ready(Some(RecordBatch::try_from_df_record_batch(
            self.schema.clone(),
            batch,
        )))
    }
}

fn build_metrics_schema() -> Schema {
    let cols = vec![
        ColumnSchema::new(
            "metric".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "kind".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "labels".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "value".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
    ];

    // Schema is always valid here
    SchemaBuilder::try_from(cols).unwrap().build().unwrap()
}

fn generate_labels(labels: HashMap<String, String>) -> String {
    serde_json::to_string(&labels).unwrap()
}

fn generate_value(value: Value) -> String {
    match value {
        Value::Counter(v) => v.to_string(),
        Value::Gauge(v) => v.to_string(),
        Value::Untyped(v) => v.to_string(),
        Value::Histogram(v) => {
            #[derive(Serialize, Deserialize)]
            struct Histogram {
                less_than: f64,
                count: f64,
            }
            let v: Vec<Histogram> = v
                .into_iter()
                .map(|h| Histogram {
                    less_than: h.less_than,
                    count: h.count,
                })
                .collect();
            serde_json::to_string(&v).unwrap()
        }
        Value::Summary(v) => {
            #[derive(Serialize, Deserialize)]
            struct Summary {
                quantile: f64,
                count: f64,
            }
            let v: Vec<Summary> = v
                .into_iter()
                .map(|h| Summary {
                    quantile: h.quantile,
                    count: h.count,
                })
                .collect();
            serde_json::to_string(&v).unwrap()
        }
    }
}
