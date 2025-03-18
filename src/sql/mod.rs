use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct QueryPlan {
    pub measurement: String,
    pub fields: Vec<String>,
    pub tags: HashMap<String, String>,
    pub time_range: Option<(i64, i64)>,
    pub limit: Option<usize>,
}

#[derive(Debug)]
pub enum QueryError {
    ParseError(String),
    UnsupportedQuery(String),
    InvalidSyntax(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            QueryError::UnsupportedQuery(msg) => write!(f, "Unsupported query: {}", msg),
            QueryError::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
        }
    }
}

impl Error for QueryError {}

pub fn parse_query(sql: &str) -> Result<QueryPlan, QueryError> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect);

    let ast =
        Parser::parse_sql(&dialect, sql).map_err(|e| QueryError::ParseError(e.to_string()))?;

    if ast.len() != 1 {
        return Err(QueryError::InvalidSyntax(
            "Only single query supported".to_string(),
        ));
    }

    let statement = &ast[0];
    match statement {
        Statement::Query(query) => parse_select_query(query),
        _ => Err(QueryError::UnsupportedQuery(
            "Only SELECT queries are supported".to_string(),
        )),
    }
}

fn parse_select_query(query: &Query) -> Result<QueryPlan, QueryError> {
    let select = match &*query.body {
        SetExpr::Select(select) => select,
        _ => {
            return Err(QueryError::UnsupportedQuery(
                "Only SELECT queries are supported".to_string(),
            ))
        }
    };

    let from = match &select.from.first() {
        Some(TableWithJoins { relation, .. }) => relation,
        _ => return Err(QueryError::InvalidSyntax("Missing FROM clause".to_string())),
    };

    let table = match from {
        TableFactor::Table { name, .. } => name,
        _ => {
            return Err(QueryError::InvalidSyntax(
                "Invalid table reference".to_string(),
            ))
        }
    };

    let measurement = table.to_string();
    let mut fields = Vec::new();
    let mut tags = HashMap::new();
    let mut time_range = None;
    let mut limit = None;

    // Parse SELECT items
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Expr::Identifier(ident) = expr {
                    fields.push(ident.to_string());
                }
            }
            _ => {
                return Err(QueryError::UnsupportedQuery(
                    "Complex expressions not supported".to_string(),
                ))
            }
        }
    }

    // Parse WHERE clause for time range and tags
    if let Some(where_clause) = &select.selection {
        parse_where_clause(where_clause, &mut tags, &mut time_range)?;
    }

    // Parse LIMIT clause
    if let Some(limit_expr) = &query.limit {
        if let Expr::Value(value) = limit_expr {
            if let Some(num) = value.to_string().parse::<usize>().ok() {
                limit = Some(num);
            }
        }
    }

    Ok(QueryPlan {
        measurement,
        fields,
        tags,
        time_range,
        limit,
    })
}

fn parse_where_clause(
    expr: &Expr,
    tags: &mut HashMap<String, String>,
    time_range: &mut Option<(i64, i64)>,
) -> Result<(), QueryError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op.to_string().as_str() {
                "AND" => {
                    parse_where_clause(left, tags, time_range)?;
                    parse_where_clause(right, tags, time_range)?;
                }
                "=" => {
                    if let (Expr::Identifier(key), Expr::Value(value)) =
                        (left.as_ref(), right.as_ref())
                    {
                        if key.to_string() == "time" {
                            // Handle time range
                            if let Some(timestamp) = parse_timestamp(value.to_string().as_str()) {
                                *time_range = Some((timestamp, timestamp));
                            }
                        } else {
                            tags.insert(key.to_string(), value.to_string());
                        }
                    }
                }
                ">=" | "<=" => {
                    if let (Expr::Identifier(key), Expr::Value(value)) =
                        (left.as_ref(), right.as_ref())
                    {
                        if key.to_string() == "time" {
                            if let Some(timestamp) = parse_timestamp(value.to_string().as_str()) {
                                match op.to_string().as_str() {
                                    ">=" => {
                                        if let Some((start, end)) = *time_range {
                                            *time_range = Some((timestamp, end));
                                        } else {
                                            *time_range = Some((timestamp, i64::MAX));
                                        }
                                    }
                                    "<=" => {
                                        if let Some((start, end)) = *time_range {
                                            *time_range = Some((start, timestamp));
                                        } else {
                                            *time_range = Some((i64::MIN, timestamp));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(QueryError::UnsupportedQuery(format!(
                        "Unsupported operator: {}",
                        op
                    )))
                }
            }
        }
        _ => {
            return Err(QueryError::UnsupportedQuery(
                "Complex WHERE clauses not supported".to_string(),
            ))
        }
    }

    Ok(())
}

fn parse_timestamp(value: &str) -> Option<i64> {
    // Remove quotes if present
    let value = value.trim_matches('"');

    // Try parsing as Unix timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        return Some(timestamp);
    }

    // Try parsing as RFC3339 timestamp
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_select() {
        let sql = "SELECT value FROM temperature WHERE tag = 'sensor1'";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert_eq!(plan.tags.get("tag").unwrap(), "sensor1");
        assert!(plan.time_range.is_none());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn test_select_with_time_range() {
        let sql = "SELECT value FROM temperature WHERE time >= 1234567890 AND time <= 1234567899";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert!(plan.tags.is_empty());
        assert_eq!(plan.time_range, Some((1234567890, 1234567899)));
        assert!(plan.limit.is_none());
    }

    #[test]
    fn test_select_with_limit() {
        let sql = "SELECT value FROM temperature LIMIT 10";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert!(plan.tags.is_empty());
        assert!(plan.time_range.is_none());
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn test_select_multiple_fields() {
        let sql = "SELECT value1, value2 FROM temperature WHERE tag = 'sensor1'";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value1", "value2"]);
        assert_eq!(plan.tags.get("tag").unwrap(), "sensor1");
        assert!(plan.time_range.is_none());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn test_select_with_multiple_tags() {
        let sql = "SELECT value FROM temperature WHERE tag1 = 'sensor1' AND tag2 = 'sensor2'";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert_eq!(plan.tags.get("tag1").unwrap(), "sensor1");
        assert_eq!(plan.tags.get("tag2").unwrap(), "sensor2");
        assert!(plan.time_range.is_none());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn test_select_with_rfc3339_timestamp() {
        let sql = "SELECT value FROM temperature WHERE time >= '2024-01-01T00:00:00Z'";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert!(plan.tags.is_empty());
        assert!(plan.time_range.is_some());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn test_select_with_all_clauses() {
        let sql = "SELECT value FROM temperature WHERE tag = 'sensor1' AND time >= 1234567890 AND time <= 1234567899 LIMIT 10";
        let plan = parse_query(sql).unwrap();

        assert_eq!(plan.measurement, "temperature");
        assert_eq!(plan.fields, vec!["value"]);
        assert_eq!(plan.tags.get("tag").unwrap(), "sensor1");
        assert_eq!(plan.time_range, Some((1234567890, 1234567899)));
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn test_invalid_sql() {
        let sql = "INVALID SQL";
        assert!(parse_query(sql).is_err());
    }

    #[test]
    fn test_unsupported_query() {
        let sql = "INSERT INTO temperature VALUES (1)";
        assert!(parse_query(sql).is_err());
    }
}
