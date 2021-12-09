pub fn query_statement_tablename(query: String) -> Result<String, String> {
    let stmt = datafusion::sql::parser::DFParser::parse_sql(&query);
    match stmt {
        Ok(t) => match &t[0] {
            datafusion::sql::parser::Statement::Statement(tt) => match tt {
                sqlparser::ast::Statement::Query(b) => match &b.body {
                    sqlparser::ast::SetExpr::Select(ss) => match &ss.from[0].relation {
                        sqlparser::ast::TableFactor::Table {
                            name,
                            alias: _,
                            args: _,
                            with_hints: _,
                        } => {
                            let tablename = &name.0[0].value;
                            return Ok(tablename.clone());
                        }
                        _ => return Err(format!("No table found")),
                    },
                    _ => return Err(format!("Unknown query: {:?}", t[0])),
                },
                _ => return Err(format!("Invalid SELECT statement: {}", tt)),
            },
            _ => return Err(format!("Invalid statement: {:?}", t)),
        },
        Err(e) => return Err(format!("Improper query: {}", e)),
    }
}
