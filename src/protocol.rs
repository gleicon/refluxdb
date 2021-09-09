use chrono::{Local};
use std::collections::HashMap;

#[derive(Debug)]
pub struct LineProtocol {
    measurement_name: String,
    tag_set: HashMap<String, String>,
    field_set: HashMap<String, String>,
    timestamp: i64,
}

impl Default for LineProtocol {
    fn default () -> LineProtocol {
        LineProtocol{
            measurement_name: "_".to_string(), 
            tag_set: HashMap::new(),
            field_set: HashMap::new(),
            timestamp: Local::now().timestamp(),
        }
    }
}

impl LineProtocol {
    pub fn new(measurement_name:  String) -> Self {
        let s = Self {
            measurement_name: measurement_name, 
            tag_set: HashMap::new(),
            field_set: HashMap::new(),
            timestamp: Local::now().timestamp(),
        };
        return s
    }

    pub fn tag(&mut self, key: String, value: String){
        if key.len() > 0 && value.len() > 0 {
            self.tag_set.insert(key, value);
        }
    }

    pub fn field(&mut self, key: String, value: String){
        if key.len() > 0 && value.len() > 0 {
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
            return Err("No FieldKey set".to_string())
        }

        let mut count = 0;

        for (k, v) in self.field_set.iter() {
            if count > 0 {
                buf +=","
            } else {
                buf +=" "
            }
            buf += &format!("{}={}", k, v);
            count+=1;
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

        let mut s = line.split_whitespace();
        // measurement name and tags
        match s.next() {
            Some(mn)=> {

                let tags = Box::new(match mn.find(",") {
                    Some(_) => { 
                        let tt = mn.split(",").collect::<Vec<&str>>();
                        tt
                    },
                    None => vec![mn],
                  });
                proto.measurement_name = tags[0].to_string();
                for tag in tags[1..].iter() {
                    match tag.split_once("=") {
                        Some((k,v)) => proto.tag(k.to_string(), v.to_string()),
                        None => (),
                    }
                }
                
            },
            None => {
                return Err(format!("Error: broken protocol line: {:?}", line));
            }
        };
        // fieldset
        match s.next() {
            Some(fk) => {
                let fkeys = Box::new(match fk.find(",") {
                    Some(_) => { 
                        let tt = fk.split(",").collect::<Vec<&str>>();
                        tt//[1..]
                    },
                    None => vec![fk],
                  });
                for fk in fkeys.iter() {
                    match fk.split_once("=") {
                        Some((k,v)) => proto.field(k.to_string(), v.to_string()),
                        None => (),
                    }
                }
            },
            None => {
                return Err(format!("Error: no fieldkey - line: {:?}", line));
            }
        }
        // timestamp
        match s.next() {
            Some(ts) => {
                proto.timestamp = ts.parse::<i64>().unwrap();
            },
            None => {
                return Err(format!("Error: no timestamp - line: {:?}", line));
            }

        }
        Ok(proto)
    }

}

#[cfg(test)]
mod tests {

    #[test]
    fn single_tag(){
        let tst = "mySingleTagMeasurement,tag1=value1 fieldKey1=\"fieldValue\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();

        assert_eq!(tst.clone(), out);

    }
    #[test]
    fn multiple_tags() {
        let tst = "myMultipleTagMeasurement,tag1=value1,tag2=value2 fieldKey=\"fieldValue\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();

        assert_eq!(tst.clone(), out);
    }

    #[test]
    fn single_fieldvalue(){
        let tst = "mySingleFieldKey fieldKey=\"fieldValue\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();

        assert_eq!(tst.clone(), out);
    }

    #[test]
    fn multiple_fieldvalues(){
        let tst = "myMultipleFieldKey fieldKey1=\"fieldValue\",fieldKey2=\"oi\" 1556813561098000000".to_string();
        let res = crate::protocol::LineProtocol::parse(tst.clone()).unwrap();
        let out = res.serialize().unwrap();

        assert_eq!(tst.clone(), out);
    }

}