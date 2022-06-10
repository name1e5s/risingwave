// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, DatabaseId, SchemaId, TableId};
use risingwave_pb::catalog::InternalStateTable as ProstInternalStateTable;

use super::column_catalog::ColumnCatalog;
use crate::optimizer::PlanRef;

#[derive(Clone, Debug, PartialEq)]
pub struct InternalStateTableCatalog {
    pub id: TableId,

    pub name: String,

    pub pks: Vec<usize>,

    pub columns: Vec<ColumnCatalog>,

    pub distribution_keys: Vec<usize>,
}

impl InternalStateTableCatalog {
    /// Get a reference to the internal state catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn to_prost(&self) -> ProstInternalStateTable {
        ProstInternalStateTable {
            id: self.id.table_id as u32,
            schema_id: SchemaId::placeholder(),
            database_id: DatabaseId::placeholder(),
            name: self.name.clone(),
            pk: self.pks.iter().map(|x| *x as _).collect(),
            columns: self.columns().iter().map(|c| c.to_protobuf()).collect(),
            distribution_keys: self
                .distribution_keys
                .iter()
                .map(|k| *k as i32)
                .collect_vec(),
        }
    }
}

pub fn infer_internal_state_table_catalog(plan_node: PlanRef) -> InternalStateTableCatalog {
    let base = plan_node.plan_base();
    let schema = &base.schema;
    let pk_indices = &base.pk_indices;
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(_, field)| ColumnCatalog {
            column_desc: ColumnDesc::from_field_without_column_id(field),
            is_hidden: false,
        })
        .collect_vec();

    InternalStateTableCatalog {
        id: TableId::placeholder(),
        name: String::from("__INTERNAL_TABLE"),
        columns,
        pks: pk_indices.clone(),
        distribution_keys: base.dist.dist_column_indices().to_vec(),
    }
}
