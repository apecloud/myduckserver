<?php

class PGTest {
    public static function main($args) {
        if (count($args) != 5) {
            echo "Usage: php " . $args[0] . " <ip> <port> <user> <password> <testFile>\n";
            exit(1);
        }

        $tests = new Tests();
        $tests->connect($args[0], intval($args[1]), $args[2], $args[3]);
        $tests->readTestsFromFile($args[4]);

        if (!$tests->runTests()) {
            $tests->disconnect();
            exit(1);
        }
        $tests->disconnect();
    }
}

class Tests {
    private $conn;
    private $tests = [];

    public function connect($ip, $port, $user, $password) {
        try {
            $url = "pgsql:host=$ip;port=$port;dbname=postgres";
            $this->conn = new PDO($url, $user, $password);
        } catch (PDOException $e) {
            throw new RuntimeException($e->getMessage());
        }
    }

    public function disconnect() {
        $this->conn = null;
    }

    public function addTest($query, $expectedResults) {
        $this->tests[] = new Test($query, $expectedResults);
    }

    public function runTests() {
        foreach ($this->tests as $test) {
            if (!$test->run($this->conn)) {
                return false;
            }
        }
        return true;
    }

    public function readTestsFromFile($filename) {
        $file = fopen($filename, "r");
        if (!$file) {
            throw new RuntimeException("Failed to open test data file");
        }

        while (($line = fgets($file)) !== false) {
            $line = trim($line);
            if (empty($line)) continue; // Skip empty lines
            $query = $line;
            $expectedResults = [];
            while (($line = fgets($file)) !== false) {
                $line = trim($line);
                if (empty($line)) break; // End of expected results for this query
                $expectedResults[] = explode(",", $line);
            }
            $this->addTest($query, $expectedResults);
        }

        fclose($file);
    }
}

class Test {
    private $query;
    private $expectedResults;

    public function __construct($query, $expectedResults) {
        $this->query = $query;
        $this->expectedResults = $expectedResults;
    }

    public function run($conn) {
        try {
            echo "Running test: " . $this->query . "\n";
            $st = $conn->prepare($this->query);
            $st->execute();
            if ($st->columnCount() == 0) {
                if (count($this->expectedResults) != 0) {
                    echo "Expected " . count($this->expectedResults) . " rows, got 0\n";
                    return false;
                }
                echo "Returns 0 rows\n";
                return true;
            }
            $rows = $st->fetchAll(PDO::FETCH_NUM);
            if (count($rows[0]) != count($this->expectedResults[0])) {
                echo "Expected " . count($this->expectedResults[0]) . " columns, got " . count($rows[0]) . "\n";
                return false;
            }
            foreach ($rows as $i => $row) {
                $row = array_map('strval', $row);
                foreach ($row as $j => $value) {
                    $value = trim($value);
                    $expectedValue = trim($this->expectedResults[$i][$j]);
                    if ($value !== $expectedValue) {
                        echo "Expected: " . $expectedValue . ", got: " . $value . "\n";
                        return false;
                    }
                }
            }
            echo "Returns " . count($rows) . " rows\n";
            if (count($rows) != count($this->expectedResults)) {
                echo "Expected " . count($this->expectedResults) . " rows\n";
                return false;
            }
            return true;
        } catch (PDOException $e) {
            echo $e->getMessage() . "\n";
            return false;
        }
    }
}

PGTest::main(array_slice($argv, 1));

?>