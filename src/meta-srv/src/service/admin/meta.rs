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

use api::v1::meta::{RangeRequest, RangeResponse};
use catalog::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix, CatalogKey, SchemaKey,
    TableGlobalKey, TABLE_GLOBAL_KEY_PREFIX,
};
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error::Result;
use crate::service::admin::HttpHandler;
use crate::service::store::ext::KvStoreExt;
use crate::service::store::kv::KvStoreRef;
use crate::{error, util};

pub struct CatalogsHandler {
    pub kv_store: KvStoreRef,
}

impl CatalogsHandler {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self { kv_store }
    }

    pub fn get_prefix() -> String {
        build_catalog_prefix()
    }

    pub fn get_catalog_from_key(key: &String) -> Result<String> {
        CatalogKey::parse(key)
            .map(|catalog| catalog.catalog_name)
            .context(error::InvalidCatalogValueSnafu)
    }
}

pub struct SchemasHandler {
    pub kv_store: KvStoreRef,
}

impl SchemasHandler {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self { kv_store }
    }

    pub fn get_prefix(catalog: impl AsRef<str>) -> String {
        build_schema_prefix(catalog)
    }

    pub fn get_schema_from_key(key: &String) -> Result<String> {
        SchemaKey::parse(key)
            .map(|schema| schema.schema_name)
            .context(error::InvalidCatalogValueSnafu)
    }
}

pub struct TablesHandler {
    pub kv_store: KvStoreRef,
}

impl TablesHandler {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self { kv_store }
    }

    pub fn get_prefix(catalog: impl AsRef<str>, schema: impl AsRef<str>) -> String {
        build_table_global_prefix(catalog, schema)
    }

    pub fn get_table_from_key(key: &String) -> Result<String> {
        TableGlobalKey::parse(key)
            .map(|table| table.table_name)
            .context(error::InvalidCatalogValueSnafu)
    }
}

pub struct TableHandler {
    pub kv_store: KvStoreRef,
}

#[async_trait::async_trait]
impl HttpHandler for CatalogsHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        get_http_response_by_prefix(
            Self::get_prefix(),
            Self::get_catalog_from_key,
            &self.kv_store,
        )
        .await
    }
}

#[async_trait::async_trait]
impl HttpHandler for SchemasHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = params
            .get("catalog_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "catalog_name",
            })?;
        get_http_response_by_prefix(
            Self::get_prefix(catalog),
            Self::get_schema_from_key,
            &self.kv_store,
        )
        .await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TablesHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = params
            .get("catalog_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "catalog_name",
            })?;

        let schema = params
            .get("schema_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "schema_name",
            })?;
        get_http_response_by_prefix(
            Self::get_prefix(catalog, schema),
            Self::get_table_from_key,
            &self.kv_store,
        )
        .await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TableHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let table_name = params
            .get("full_table_name")
            .map(|full_table_name| full_table_name.replace('.', "-"))
            .context(error::MissingRequiredParameterSnafu {
                param: "full_table_name",
            })?;
        let table_key = format!("{TABLE_GLOBAL_KEY_PREFIX}-{table_name}");

        let response = self.kv_store.get(table_key.into_bytes()).await?;
        let mut value: String = "Not found result".to_string();
        if let Some(key_value) = response {
            value = String::from_utf8(key_value.value).context(error::InvalidUtf8ValueSnafu)?;
        }
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(value)
            .context(error::InvalidHttpBodySnafu)
    }
}

/// Get kv_store's key list with http response format by prefix key
async fn get_http_response_by_prefix(
    key_prefix: String,
    parser: fn(s: &String) -> Result<String>,
    kv_store: &KvStoreRef,
) -> Result<http::Response<String>> {
    let keys = get_keys_by_prefix(key_prefix, parser, kv_store).await?;
    let body = serde_json::to_string(&keys).context(error::SerializeToJsonSnafu {
        input: format!("{keys:?}"),
    })?;

    http::Response::builder()
        .status(http::StatusCode::OK)
        .body(body)
        .context(error::InvalidHttpBodySnafu)
}

/// Get kv_store's key list by prefix key
async fn get_keys_by_prefix(
    key_prefix: String,
    parser: fn(s: &String) -> Result<String>,
    kv_store: &KvStoreRef,
) -> Result<Vec<String>> {
    let key_prefix_u8 = key_prefix.clone().into_bytes();
    let range_end = util::get_prefix_end_key(&key_prefix_u8);
    let req = RangeRequest {
        key: key_prefix_u8,
        range_end,
        keys_only: true,
        ..Default::default()
    };

    let response: RangeResponse = kv_store.range(req).await?;

    let kvs = response.kvs;
    //let result = kvs.iter().map(parser).collect::<Result<Vec<_>, _>>();
    let mut values = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let value = String::from_utf8(kv.key).context(error::InvalidUtf8ValueSnafu)?;
        let value = parser(&value)?;
        values.push(value)
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::PutRequest;
    use catalog::helper::{CatalogKey, SchemaKey, TableGlobalKey};

    use crate::service::admin::meta::{
        get_keys_by_prefix, CatalogsHandler, SchemasHandler, TablesHandler,
    };
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_get_list_by_prefix() {
        let in_mem = Arc::new(MemStore::new()) as KvStoreRef;
        let catalog_name = "test_catalog";
        let schema_name = "test_schema";
        let table_name = "test_table";
        let catalog = CatalogKey {
            catalog_name: catalog_name.to_string(),
        };
        in_mem
            .put(PutRequest {
                key: catalog.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let schema = SchemaKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        };
        in_mem
            .put(PutRequest {
                key: schema.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let table1 = TableGlobalKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        };
        let table2 = TableGlobalKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: "test_table1".to_string(),
        };
        in_mem
            .put(PutRequest {
                key: table1.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        in_mem
            .put(PutRequest {
                key: table2.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let catalog_key = get_keys_by_prefix(
            CatalogsHandler::get_prefix(),
            CatalogsHandler::get_catalog_from_key,
            &in_mem,
        )
        .await
        .unwrap();
        let schema_key = get_keys_by_prefix(
            SchemasHandler::get_prefix(schema.catalog_name),
            SchemasHandler::get_schema_from_key,
            &in_mem,
        )
        .await
        .unwrap();
        let table_key = get_keys_by_prefix(
            TablesHandler::get_prefix(table1.catalog_name, table1.schema_name),
            TablesHandler::get_table_from_key,
            &in_mem,
        )
        .await
        .unwrap();

        assert_eq!(catalog_name, catalog_key[0]);
        assert_eq!(schema_name, schema_key[0]);
        assert_eq!(table_name, table_key[0]);
        assert_eq!("test_table1", table_key[1]);
    }
}
