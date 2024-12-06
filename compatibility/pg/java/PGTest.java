import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.LinkedList;

public class PGTest {
    public static class Tests {
        private Connection conn;
        private Statement st;
        private List<Test> tests = new LinkedList<>();

        public void connect(String ip, int port, String user, String password) {
            try {
                String url = "jdbc:postgresql://" + ip + ":" + port + "/postgres";
                conn = DriverManager.getConnection(url, user, password);
                st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public void disconnect() {
            try {
                st.close();
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public void addTest(String query, String[][] expectedResults) {
            tests.add(new Test(query, expectedResults));
        }

        public boolean runTests() {
            for (Test test : tests) {
                if (!test.run()) {
                    return false;
                }
            }
            return true;
        }

        public void readTestsFromFile(String filename) {
            try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String query = line;
                    List<String[]> results = new LinkedList<>();
                    while ((line = br.readLine()) != null && !line.trim().isEmpty()) {
                        results.add(line.split(","));
                    }
                    String[][] expectedResults = results.toArray(new String[0][]);
                    addTest(query, expectedResults);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public class Test {
            private String query;
            private String[][] expectedResults;

            public Test(String query, String[][] expectedResults) {
                this.query = query;
                this.expectedResults = expectedResults;
            }

            public boolean run() {
                try {
                    System.out.println("Running test: " + query);
                    if (!st.execute(query)) {
                        if (expectedResults.length != 0) {
                            System.err.println("Expected " + expectedResults.length + " rows, got 0");
                            return false;
                        }
                        System.out.println("Returns 0 rows");
                        return true;
                    }
                    ResultSet rs = st.getResultSet();
                    if (rs.getMetaData().getColumnCount() != expectedResults[0].length) {
                        System.err.println("Expected " + expectedResults[0].length + " columns, got " + rs.getMetaData().getColumnCount());
                        return false;
                    }
                    int rows = 0;
                    while (rs.next()) {
                        int cols = 0;
                        for (String expected : expectedResults[rows]) {
                            String result = rs.getString(cols + 1).trim();
                            if (!expected.equals(result)) {
                                System.err.println("Expected:\n'" + expected + "'");
                                System.err.println("Result:\n'" + result + "'");
                                return false;
                            }
                            cols++;
                        }
                        rows++;
                    }

                    System.out.println("Returns " + rows + " rows");
                    if (rows != expectedResults.length) {
                        System.err.println("Expected " + expectedResults.length + " rows");
                        return false;
                    }
                    return true;
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                    return false;
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: java PGTest <ip> <port> <user> <password> <testFile>");
            System.exit(1);
        }

        Tests tests = new Tests();
        tests.connect(args[0], Integer.parseInt(args[1]), args[2], args[3]);
        tests.readTestsFromFile(args[4]);

        if (!tests.runTests()) {
            tests.disconnect();
            System.exit(1);
        }
        tests.disconnect();
    }
}