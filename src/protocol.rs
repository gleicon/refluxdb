use chrono::Local;
use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub struct LineProtocol {
    pub measurement_name: String,
    pub tag_set: IndexMap<String, String>,
    pub field_set: IndexMap<String, String>,
    pub timestamp: i64,
}

#[test]
fn parse_empty_string() {
    let tst = "".to_string();
    let res = crate::protocol::LineProtocol::parse(tst);
    assert!(res.is_err());
}

#[test]
fn parse_missing_fieldkey() {
    let tst = "measurement,tag1=value1".to_string();
    let res = crate::protocol::LineProtocol::parse(tst);
    assert!(res.is_err());
}

#[test]
fn parse_missing_timestamp() {
    let tst = "measurement,tag1=value1 fieldKey=\"fieldValue\"".to_string();
    let res = crate::protocol::LineProtocol::parse(tst);
    assert!(res.is_err());
}

#[test]
fn serialize_no_fieldkey() {
    let mut proto = crate::protocol::LineProtocol::default();
    proto.measurement_name = "measurement".to_string();
    proto.tag("tag1".to_string(), "value1".to_string());
    let res = proto.serialize();
    assert!(res.is_err());
}

impl Default for LineProtocol {
    fn default() -> LineProtocol {
        LineProtocol {
            measurement_name: "_".to_string(),
            tag_set: IndexMap::new(),
            field_set: IndexMap::new(),
            timestamp: Local::now().timestamp(),
        }
    }
}

impl LineProtocol {
    // pub fn new(measurement_name: String) -> Self {
    //     let s = Self {
    //         measurement_name: measurement_name,
    //         tag_set: IndexMap::new(),
    //         field_set: IndexMap::new(),
    //         timestamp: Local::now().timestamp(),
    //     };
    //     return s;
    // }

    pub fn tag(&mut self, key: String, value: String) {
        if key.len() > 0 && value.len() > 0 {
            self.tag_set.insert(key, value);
        }
    }

    pub fn field(&mut self, key: String, value: String) {
        // Allow empty values but not empty keys
        if key.len() > 0 {
            self.field_set.insert(key, value);
        }
    }

    pub fn serialize(self: Self) -> Result<String, String> {
        let mut buf = format!("{}", self.measurement_name);
        if !self.tag_set.is_empty() {
            for (k, v) in self.tag_set.iter() {
                buf += &format!(",{}={}", k, v);
            }
        }

        if self.field_set.is_empty() {
            return Err("No FieldKey set".to_string());
        }

        let mut count = 0;
        for (k, v) in self.field_set.iter() {
            if count > 0 {
                buf += ","
            } else {
                buf += " "
            }
            // Simple quote wrapping without escaping
            buf += &format!("{}=\"{}\"", k, v);
            count += 1;
        }

        buf += &format!(" {}", self.timestamp);
        Ok(buf)
    }

    // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
    // <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
    // myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000

    pub fn parse(line: String) -> Result<Self, String> {
        if line.is_empty() {
            return Err("Error: Empty string".to_string());
        }

        let mut proto = LineProtocol::default();

        // Split on whitespace but preserve the original line for error messages
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(format!("Error: invalid protocol line: {:?}", line));
        }

        // Parse measurement name and tags
        let mn = parts[0];
        let tags = Box::new(match mn.find(",") {
            Some(_) => mn.split(",").map(|s| s.trim()).collect::<Vec<&str>>(),
            None => vec![mn],
        });

        if tags[0].is_empty() {
            return Err("Error: Empty measurement name".to_string());
        }
        proto.measurement_name = tags[0].to_string();

        for tag in tags[1..].iter() {
            if let Some((k, v)) = tag.split_once("=") {
                if k.is_empty() {
                    return Err("Error: Empty tag key".to_string());
                }
                if v.is_empty() {
                    return Err("Error: Empty tag value".to_string());
                }
                proto.tag(k.to_string(), v.to_string());
            }
        }

        // Parse fields
        let fk = parts[1];
        let fkeys = Box::new(match fk.find(",") {
            Some(_) => fk.split(",").map(|s| s.trim()).collect::<Vec<&str>>(),
            None => vec![fk],
        });

        for fk in fkeys.iter() {
            if let Some((k, v)) = fk.split_once("=") {
                if k.is_empty() {
                    return Err("Error: Empty field key".to_string());
                }
                // Check if the field value is properly quoted
                if !v.starts_with('"') || !v.ends_with('"') {
                    return Err(format!("Error: field value must be quoted: {:?}", line));
                }
                // Simple quote removal
                let cleaned_value = v[1..v.len() - 1].to_string();
                proto.field(k.to_string(), cleaned_value);
            }
        }

        // Parse timestamp
        if parts.len() < 3 {
            return Err(format!("Error: no timestamp - line: {:?}", line));
        }

        let ts = parts[2];
        proto.timestamp = match ts.parse::<i64>() {
            Ok(a) => a,
            Err(e) => {
                return Err(format!(
                    "Error: invalid timestamp: {} - line: {:?}",
                    e, line
                ))
            }
        };

        Ok(proto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_tag() {
        let tst = "mySingleTagMeasurement,tag1=value1 fieldKey1=\"fieldValue\" 1556813561098000000"
            .to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn multiple_tags() {
        let tst = "myMultipleTagMeasurement,tag1=value1,tag2=value2 fieldKey=\"fieldValue\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn single_fieldvalue() {
        let tst = "mySingleFieldKey fieldKey=\"fieldValue\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn multiple_fieldvalues() {
        let tst =
            "myMultipleFieldKey fieldKey1=\"fieldValue\",fieldKey2=\"oi\" 1556813561098000000"
                .to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_empty_measurement_name() {
        let tst = ",tag1=value1 fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_invalid_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\" invalid_timestamp".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_missing_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\"".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_empty_field_value() {
        let tst = "measurement,tag1=value1 fieldKey=\"\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_multiple_spaces() {
        let tst = "measurement  fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(out, "measurement fieldKey=\"value\" 1556813561098000000");
    }

    #[test]
    fn test_empty_tags() {
        let tst = "measurement fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_invalid_field_format() {
        let tst = "measurement,tag1=value1 fieldKey=value 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_invalid_tag_format() {
        let tst = "measurement,tag1 value1 fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_very_large_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\" 9223372036854775807".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_negative_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\" -1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_empty_tag_key() {
        let tst = "measurement,=value1 fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_empty_tag_value() {
        let tst = "measurement,tag1= fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_empty_field_key() {
        let tst = "measurement,tag1=value1 =\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_missing_field_value() {
        let tst = "measurement,tag1=value1 fieldKey= 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_malformed_quotes() {
        let tst = "measurement,tag1=value1 fieldKey=\"value 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst);
        assert!(res.is_err());
    }

    #[test]
    fn test_zero_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\" 0".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_small_timestamp() {
        let tst = "measurement,tag1=value1 fieldKey=\"value\" 1".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_multiple_spaces_in_tags() {
        let tst = "measurement,tag1=value1, tag2=value2 fieldKey=\"value\" 1556813561098000000"
            .to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(
            out,
            "measurement,tag1=value1,tag2=value2 fieldKey=\"value\" 1556813561098000000"
        );
    }

    #[test]
    fn test_multiple_spaces_in_fields() {
        let tst = "measurement,tag1=value1 fieldKey1=\"value1\", fieldKey2=\"value2\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(
            out,
            "measurement,tag1=value1 fieldKey1=\"value1\",fieldKey2=\"value2\" 1556813561098000000"
        );
    }

    #[test]
    fn test_measurement_with_spaces() {
        let tst = "my measurement,tag1=value1 fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_tag_with_spaces() {
        let tst = "measurement,tag 1=value1 fieldKey=\"value\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_field_value_with_spaces() {
        let tst = "measurement,tag1=value1 fieldKey=\"value with spaces\" 1556813561098000000"
            .to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }

    #[test]
    fn test_tag_value_with_spaces() {
        let tst = "measurement,tag1=\"value with spaces\" fieldKey=\"value\" 1556813561098000000"
            .to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();
        assert_eq!(tst, out);
    }
}
