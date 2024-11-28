use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::process::exit;

extern crate postgres;
use postgres::{Client, NoTls, Error};

struct Test {
    query: String,
    expected_results: Vec<Vec<String>>,
}

impl Test {
    fn new(query: String, expected_results: Vec<Vec<String>>) -> Self {
        Test { query, expected_results }
    }

    fn run(&self, client: &mut Client) -> bool {
        println!("Running test: {}", self.query);
        match client.query(&self.query, &[]) {
            Ok(rows) => {
                if rows.is_empty() {
                    if self.expected_results.is_empty() {
                        println!("Returns 0 rows");
                        return true;
                    }
                    eprintln!("Expected {} rows, got 0", self.expected_results.len());
                    return false;
                }
                if rows[0].columns().len() != self.expected_results[0].len() {
                    eprintln!("Expected {} columns, got {}", self.expected_results[0].len(), rows[0].columns().len());
                    return false;
                }
                for (i, row) in rows.iter().enumerate() {
                    for (j, expected) in self.expected_results[i].iter().enumerate() {
                        let result: String = row.get(j);
                        if expected != &result {
                            eprintln!("Expected:\n'{}'", expected);
                            eprintln!("Result:\n'{}'\nRest of the results:", result);
                            for row in rows.iter().skip(i + 1) {
                                eprintln!("{}", row.get::<usize, String>(0));
                            }
                            return false;
                        }
                    }
                }
                println!("Returns {} rows", rows.len());
                if rows.len() != self.expected_results.len() {
                    eprintln!("Expected {} rows", self.expected_results.len());
                    return false;
                }
                true
            }
            Err(err) => {
                eprintln!("{}", err);
                false
            }
        }
    }
}

struct Tests {
    client: Client,
    tests: Vec<Test>,
}

impl Tests {
    fn new(ip: &str, port: u16, user: &str, password: &str) -> Result<Self, Error> {
        let conn_str = format!("host={} port={} user={} password={} dbname=postgres", ip, port, user, password);
        let client = Client::connect(&conn_str, NoTls)?;
        Ok(Tests { client, tests: Vec::new() })
    }

    fn add_test(&mut self, query: String, expected_results: Vec<Vec<String>>) {
        self.tests.push(Test::new(query, expected_results));
    }

    fn run_tests(&mut self) -> bool {
        for test in &self.tests {
            if !test.run(&mut self.client) {
                return false;
            }
        }
        true
    }

    fn read_tests_from_file(&mut self, filename: &str) -> io::Result<()> {
        let file = File::open(filename)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        while let Some(Ok(line)) = lines.next() {
            if line.trim().is_empty() {
                continue;
            }
            let query = line;
            let mut results = Vec::new();
            while let Some(Ok(line)) = lines.next() {
                if line.trim().is_empty() {
                    break;
                }
                results.push(line.split(',').map(String::from).collect());
            }
            self.add_test(query, results);
        }
        Ok(())
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!("Usage: {} <ip> <port> <user> <password> <testFile>", args[0]);
        exit(1);
    }

    let ip = &args[1];
    let port: u16 = args[2].parse().expect("Invalid port number");
    let user = &args[3];
    let password = &args[4];
    let test_file = &args[5];

    let mut tests = Tests::new(ip, port, user, password).expect("Failed to connect to database");
    tests.read_tests_from_file(test_file).expect("Failed to read test file");

    if !tests.run_tests() {
        exit(1);
    }
}