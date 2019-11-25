extern crate redis;

mod eval_args;
mod importcsv;
mod importredis;

use std::{
    io::{self, Write},
    collections::HashMap,
};

fn input(prompt: &str) -> String {
    print!("{}", prompt);
    let _ = io::stdout().flush().expect("Could not flush output!");
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("Could not read input!");
    input.trim().to_string()
}

fn set_data(map: &HashMap<String, String>, key: &String) -> Option<String> {
    map.get(key).map(|s| s.clone())
}

fn set_data_or_input(map: &HashMap<String, String>, key: &String, prompt: &str) -> String {
    set_data(map, key).unwrap_or_else(|| input(prompt))
}

fn main() {
    // Declaring cmdline argument options
    let str_file = String::from("file");
    let str_name = String::from("name");
    let str_template = String::from("template");
    let str_ip = String::from("address");
    let str_port = String::from("port");
    let str_dbname = String::from("dbname");
    let str_identifier = String::from("identifier");
    let str_verbose = String::from("verbose");
    let str_help = String::from("help");

    // Checking args on those options
    let args: Vec<String> = std::env::args().collect();
    let param_options = vec![
        str_file.clone(),
        str_name.clone(),
        str_template.clone(),
        str_ip.clone(),
        str_port.clone(),
        str_dbname.clone(),
        str_identifier.clone(),
        str_verbose.clone(),
        str_help.clone()
    ];
    let params = eval_args::evaluate(args, &param_options);

    // Printing out help screen and exiting after
    if let Some(_x) = params.get(&str_help) {
        println!("OPTIONS:");
        for param in param_options {
            println!("\t--{}  /  -{}", param, param.chars().next().unwrap());
        }
        return;
    }

    // Debug output of matched params
    if let Some(_x) = params.get(&str_verbose) {
        println!("Params:");
        for (key, val) in params.iter() {
            println!("{}: {}", key, val);
        }
    }

    // Assign Program parameters
    let filename = set_data_or_input(&params, &str_file, "Enter filepath: ");
    let mut template = set_data_or_input(&params, &str_template, "Enter template filepath: ");
    let name = set_data_or_input(&params, &str_name, "Enter collection name: ");
    let ip_addr = set_data_or_input(&params, &str_ip, "Enter server ip: ");
    let port = set_data(&params, &str_port).map(|s| s.parse::<u16>().expect("Invalid port number!"));
    let dbname = set_data(&params, &str_dbname);
    let id_key = set_data_or_input(&params, &str_identifier, "Enter identifying key: ");

    // Check if template is not additional, but header line in the file proper
    let header = match template.as_str() {
        "header" => true,
        _ => false,
    };
    if header {
        template = filename.clone();
    }

    // Map template keys to file data
    let mut mapped_data: Vec<HashMap<String, String>> = Vec::new();
    {
        let data = importcsv::read_csv(filename.as_str(), header).expect("Could not read import data file!");
        let mapping = importcsv::read_template(template.as_str()).expect("Could not read template file!");

        for row in &data {
            let mut hash = HashMap::new();
            for (col, key) in row.iter().zip(mapping.iter()) {
                hash.insert(key.clone(), col.clone());
            }
            mapped_data.push(hash);
        }
    }

    // Import data to redis database
    let len = mapped_data.len();
    match importredis::import(ip_addr, port, dbname, name, id_key, mapped_data) {
        Ok(_) => println!("Successfully imported {} rows to redis!", len),
        Err(e) => println!("Error occurred while trying to import row: {} / {}", e.description(), e),
    };
}
