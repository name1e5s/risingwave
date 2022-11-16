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

pub mod vec_datum;

pub use vec_datum::{Row, RowDeserializer};

/// `CompactedRow` is used in streaming executors' cache, which takes less memory than `Vec<Datum>`.
/// Executors need to serialize Row into `CompactedRow` before writing into cache.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CompactedRow {
    pub row: Vec<u8>,
}

impl From<&Row> for CompactedRow {
    fn from(row: &Row) -> Self {
        Self {
            row: row.serialize(&None),
        }
    }
}