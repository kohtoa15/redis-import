use std::{
    collections::HashMap,
};

pub fn evaluate(args: Vec<String>, keys: &Vec<String>) -> HashMap<String, String> {
    let mut ret: HashMap<String, String> = HashMap::new();

    let mut current_key: Option<String> = None;

    for arg in args {
        match check_key(arg.clone(), keys) {
            Some(key) => {
                current_key = Some(key.clone());
                ret.insert(key, String::new());
            },
            None => {
                if let Some(key) = current_key.clone() {
                    ret.insert(key, arg);
                }
            },
        };
    }

    return ret;
}

fn check_key(arg: String, keys: &Vec<String>) -> Option<String> {
    for key in keys.iter() {
        // Long Version || Short Version
        if arg == format!("--{}", key.clone()) || arg == format!("-{}", key.as_str().chars().next().unwrap()) {
            return Some(key.clone());
        }
    }
    return None;
}
