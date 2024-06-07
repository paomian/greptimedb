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

use std::collections::HashMap;

use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use axum::extract::{Json, Query, State};
use axum::headers::ContentType;
use axum::Extension;
use common_telemetry::{error, info};
use headers::HeaderMapExt;
use http::HeaderMap;
use mime_guess::mime;
use pipeline::error::{CastTypeSnafu, ExecPipelineSnafu};
use pipeline::Value as PipelineValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    InvalidParameterSnafu, ParseJsonSnafu, PipelineSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::query_handler::LogHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogIngesterQueryParams {
    pub table_name: Option<String>,
    pub db: Option<String>,
    pub pipeline_name: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn add_pipeline(
    State(handler): State<LogHandlerRef>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Json(payload): Json<Value>,
) -> Result<String> {
    let name = payload["name"].as_str().context(InvalidParameterSnafu {
        reason: "name is required in payload",
    })?;
    let pipeline = payload["pipeline"]
        .as_str()
        .context(InvalidParameterSnafu {
            reason: "pipeline is required in payload",
        })?;

    let content_type = "yaml";
    let result = handler
        .insert_pipeline(name, content_type, pipeline, query_ctx)
        .await;

    result.map(|_| "ok".to_string()).map_err(|e| {
        error!(e; "failed to insert pipeline");
        e
    })
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(handler): State<LogHandlerRef>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContextRef>,
    // TypedHeader(content_type): TypedHeader<ContentType>,
    headers: HeaderMap,
    payload: String,
) -> Result<HttpResponse> {
    // TODO(shuiyisong): remove debug log
    info!("[log_header]: {:?}", headers);
    info!("[log_payload]: {:?}", payload);

    if payload == "{\"access_validation\":true}" || payload == "0,access_validation" {
        return Ok(HttpResponse::GreptimedbV1(GreptimedbV1Response {
            output: vec![],
            execution_time_ms: 0,
            resp_metrics: HashMap::new(),
        }));
    }

    let content_type = headers
        .typed_get::<ContentType>()
        .unwrap_or(ContentType::text());

    let pipeline_name = query_params.pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })?;
    let table_name = query_params.table_name.context(InvalidParameterSnafu {
        reason: "table_name is required",
    })?;

    let m: mime::Mime = content_type.clone().into();
    let value = match m.subtype() {
        // TODO (qtang): we should decide json or jsonl
        mime::JSON => serde_json::from_str(&payload).context(ParseJsonSnafu)?,
        // TODO (qtang): we should decide which content type to support
        // form_url_cncoded type is only placeholder
        mime::WWW_FORM_URLENCODED => parse_space_separated_log(payload)?,
        // add more content type support
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    };

    match log_ingester_inner(handler, pipeline_name, table_name, value, query_ctx).await {
        Ok(response) => Ok(response),
        Err(e) => {
            error!(e; "[log_other_err]: failed to insert log");
            Ok(HttpResponse::GreptimedbV1(GreptimedbV1Response {
                output: vec![],
                execution_time_ms: 0,
                resp_metrics: HashMap::new(),
            }))
        }
    }
}

fn parse_space_separated_log(payload: String) -> Result<Value> {
    // ToStructuredLogSnafu
    let _log = payload.split_whitespace().collect::<Vec<&str>>();
    // TODO (qtang): implement this
    todo!()
}

async fn log_ingester_inner(
    state: LogHandlerRef,
    pipeline_name: String,
    table_name: String,
    payload: Value,
    query_ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let start = std::time::Instant::now();
    let pipeline_data = PipelineValue::try_from(payload)
        .map_err(|reason| CastTypeSnafu { msg: reason }.build())
        .context(PipelineSnafu)?;

    let pipeline = state
        .get_pipeline(&pipeline_name, query_ctx.clone())
        .await?;
    let transformed_data: Rows = pipeline
        .exec(pipeline_data)
        .map_err(|reason| ExecPipelineSnafu { reason }.build())
        .context(PipelineSnafu)?;

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    let output = state.insert_log(insert_requests, query_ctx).await;
    match output {
        Ok(_) => Ok(GreptimedbV1Response::from_output(vec![output])
            .await
            .with_execution_time(start.elapsed().as_millis() as u64)),
        Err(e) => {
            error!(e; "[log_insert_err]: failed to insert log in storage engine.");
            Ok(HttpResponse::GreptimedbV1(GreptimedbV1Response {
                output: vec![],
                execution_time_ms: 0,
                resp_metrics: HashMap::new(),
            }))
        }
    }
    // Ok(GreptimedbV1Response::from_output(vec![output])
    //     .await
    //     .with_execution_time(start.elapsed().as_millis() as u64))
}
