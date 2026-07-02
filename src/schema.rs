use anyhow::Result;
use crate::ods::{self, Desc, OdsReader};

fn fb_type_to_pg(desc: &Desc) -> String {
    match desc.dtype {
        ods::DTYPE_TEXT | ods::DTYPE_CSTRING => format!("VARCHAR({})", desc.length),
        ods::DTYPE_VARYING                   => format!("VARCHAR({})", desc.length.saturating_sub(2)),
        ods::DTYPE_SHORT    => if desc.scale == 0 { "SMALLINT".into() } else { "DOUBLE PRECISION".into() },
        ods::DTYPE_LONG     => if desc.scale == 0 { "INTEGER".into()  } else { "DOUBLE PRECISION".into() },
        ods::DTYPE_INT64 | ods::DTYPE_QUAD
                            => if desc.scale == 0 { "BIGINT".into()   } else { "DOUBLE PRECISION".into() },
        ods::DTYPE_REAL                        => "REAL".into(),
        ods::DTYPE_DOUBLE | ods::DTYPE_D_FLOAT => "DOUBLE PRECISION".into(),
        ods::DTYPE_SQL_DATE                    => "DATE".into(),
        ods::DTYPE_SQL_TIME | ods::DTYPE_SQL_TIME_TZ     => "TIME".into(),
        ods::DTYPE_TIMESTAMP | ods::DTYPE_TIMESTAMP_TZ   => "TIMESTAMP".into(),
        ods::DTYPE_BOOLEAN                     => "BOOLEAN".into(),
        ods::DTYPE_DBKEY                       => "BYTEA".into(),
        _                                      => "TEXT".into(),
    }
}

/// Build CREATE TABLE SQL from an already-open ODS reader.
pub fn create_table_sql_from(db: &OdsReader, table: &str, unlogged: bool) -> Result<String> {
    let relation_id = db.find_relation_id(table)?;
    let descs = db.read_format(relation_id, u16::MAX)?;
    let field_order: Vec<(usize, String)> = db.read_field_names(relation_id, table)
        .unwrap_or_else(|_| (0..descs.len()).map(|i| (i, format!("col_{i}"))).collect());

    let pk_fields = db.read_primary_key_fields(table).ok()
        .filter(|v| !v.is_empty());

    let pg_table = table.to_lowercase();
    let kw       = if unlogged { "UNLOGGED " } else { "" };
    let mut lines: Vec<String> = field_order.iter().map(|(fid, name)| {
        let desc    = descs.get(*fid).cloned().unwrap_or_else(Desc::default_zero);
        let pg_type = fb_type_to_pg(&desc);
        let col     = format!("\"{}\"", name.to_lowercase());
        format!("    {:<42} {}", col, pg_type)
    }).collect();

    if let Some(ref pks) = pk_fields {
        let pk_cols = pks.iter()
            .map(|n| format!("\"{}\"", n.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("    PRIMARY KEY ({})", pk_cols));
    }

    let body = lines.join(",\n");
    let sql = format!("CREATE {}TABLE \"{}\" (\n{}\n);", kw, pg_table, body);
    Ok(sql)
}

/// Convenience wrapper that opens the ODS file from args.
pub fn create_table_sql(args: &crate::Args) -> Result<String> {
    let db    = OdsReader::open(args.database.as_deref().unwrap())?;
    let table = args.table.as_deref().unwrap();
    create_table_sql_from(&db, table, false)
}
