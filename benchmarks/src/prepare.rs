// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;

use crate::utils::{open_duckdb_conn, validate_output};

#[derive(Parser)]
pub struct PrepareDataset {
    /// Dataset to prepare.
    #[arg(long)]
    pub dataset: String,

    /// Output path for CSV files. The command writes to `{output}/{table}/`.
    #[arg(long)]
    pub output: String,

    /// Number of rows to materialize.
    #[arg(long)]
    pub rows: u64,

    /// Print the selected source files and destination without writing.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Deserialize)]
struct SourceRecipe {
    kind: SourceKind,
    table: String,
    base_url: String,
    #[serde(default)]
    url_query: Option<String>,
    columns: Vec<String>,
    approx_rows_per_file: u64,
    #[serde(default)]
    extra_files: usize,
    sequences: Vec<SourceSequence>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    ParquetSequence,
}

#[derive(Deserialize)]
struct SourceSequence {
    path: String,
    file_count: usize,
}

impl PrepareDataset {
    pub fn prepare(self) -> Result<()> {
        if self.rows == 0 {
            bail!("--rows must be greater than 0");
        }

        let recipe_path = format!("datasets/{}/source.toml", self.dataset);
        let recipe = SourceRecipe::try_from(recipe_path.as_str())?;
        let conn = open_duckdb_conn()?;
        let output = self.output.trim_end_matches('/');
        let urls = recipe.source_urls(self.rows);

        println!(
            "Preparing {rows} rows for dataset '{dataset}' from {files} parquet file(s).",
            rows = self.rows,
            dataset = self.dataset,
            files = urls.len()
        );

        if urls.is_empty() {
            bail!("No source files configured in {recipe_path}");
        }

        if self.dry_run {
            println!("Output path: {output}/{table}", table = recipe.table);
            println!("First source file: {}", urls[0]);
            println!("Dry run complete. No files were written.");
            return Ok(());
        }

        if !output.contains("://") {
            std::fs::create_dir_all(output)
                .with_context(|| format!("Failed to create output directory '{output}'"))?;
        }

        validate_output(std::iter::once(recipe.table.as_str()), &conn, output)?;

        let read_parquet_urls = urls
            .iter()
            .map(|url| format!("'{}'", url.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        let columns = recipe.columns.join(", ");
        let sql = format!(
            "COPY (\
            SELECT {columns} \
            FROM read_parquet([{read_parquet_urls}]) \
            LIMIT {rows}\
        ) TO '{output}/{table}' (FORMAT CSV, HEADER true, PER_THREAD_OUTPUT true);",
            rows = self.rows,
            table = recipe.table,
        );
        conn.execute_batch(&sql)
            .with_context(|| format!("Failed to prepare CSV data for '{}'", self.dataset))?;

        let csv_count: u64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM read_csv('{output}/{table}/*.csv', parallel=false, header=true)",
                    table = recipe.table,
                ),
                [],
                |row| row.get(0),
            )
            .with_context(|| format!("Failed to count prepared CSV rows for '{}'", self.dataset))?;

        if csv_count != self.rows {
            bail!(
                "Prepared {csv_count} rows for {table}, expected {expected}",
                table = recipe.table,
                expected = self.rows
            );
        }

        println!(
            "Prepared {csv_count} rows at {output}/{table}.",
            table = recipe.table
        );
        Ok(())
    }
}

impl TryFrom<&str> for SourceRecipe {
    type Error = anyhow::Error;

    fn try_from(path: &str) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("Failed to read '{path}'"))?;
        let recipe: SourceRecipe =
            toml::from_str(&content).with_context(|| format!("Failed to parse '{path}'"))?;

        match recipe.kind {
            SourceKind::ParquetSequence => {}
        }
        if recipe.table.is_empty() {
            bail!("Source recipe '{path}' must set table");
        }
        if recipe.columns.is_empty() {
            bail!("Source recipe '{path}' must set at least one column");
        }
        if recipe.approx_rows_per_file == 0 {
            bail!("Source recipe '{path}' must set approx_rows_per_file greater than 0");
        }
        Ok(recipe)
    }
}

impl SourceRecipe {
    fn source_urls(&self, rows: u64) -> Vec<String> {
        let required_files = rows
            .div_ceil(self.approx_rows_per_file)
            .saturating_add(self.extra_files as u64) as usize;
        let mut urls = Vec::with_capacity(required_files);

        for sequence in &self.sequences {
            for i in 0..sequence.file_count {
                let mut url = format!(
                    "{base}/{path}/{i:04}.parquet",
                    base = self.base_url.trim_end_matches('/'),
                    path = sequence.path.trim_matches('/')
                );
                if let Some(query) = &self.url_query {
                    url.push('?');
                    url.push_str(query);
                }
                urls.push(url);
                if urls.len() >= required_files {
                    return urls;
                }
            }
        }

        urls
    }
}
