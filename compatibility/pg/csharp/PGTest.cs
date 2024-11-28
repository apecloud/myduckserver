using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.IO;

public class PGTest
{
    public class Tests
    {
        private SqlConnection conn;
        private SqlCommand cmd;
        private List<Test> tests = new List<Test>();

        public void Connect(string ip, int port, string user, string password)
        {
            string connectionString = $"Server={ip},{port};User Id={user};Password={password};";
            try
            {
                conn = new SqlConnection(connectionString);
                conn.Open();
                cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
            }
            catch (SqlException e)
            {
                throw new Exception($"Error connecting to database: {e.Message}", e);
            }
        }

        public void Disconnect()
        {
            try
            {
                cmd.Dispose();
                conn.Close();
            }
            catch (SqlException e)
            {
                throw new Exception(e.Message);
            }
        }

        public void AddTest(string query, string[][] expectedResults)
        {
            tests.Add(new Test(query, expectedResults));
        }

        public bool RunTests()
        {
            foreach (var test in tests)
            {
                if (!test.Run(cmd))
                {
                    return false;
                }
            }
            return true;
        }

        public void ReadTestsFromFile(string filename)
        {
            try
            {
                using (var reader = new StreamReader(filename))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;
                        string query = line;
                        var results = new List<string[]>();
                        while ((line = reader.ReadLine()) != null && !string.IsNullOrWhiteSpace(line))
                        {
                            results.Add(line.Split(','));
                        }
                        string[][] expectedResults = results.ToArray();
                        AddTest(query, expectedResults);
                    }
                }
            }
            catch (IOException e)
            {
                Console.Error.WriteLine(e.Message);
                Environment.Exit(1);
            }
        }

        public class Test
        {
            private string query;
            private string[][] expectedResults;

            public Test(string query, string[][] expectedResults)
            {
                this.query = query;
                this.expectedResults = expectedResults;
            }

            public bool Run(SqlCommand cmd)
            {
                try
                {
                    Console.WriteLine("Running test: " + query);
                    cmd.CommandText = query;
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (!reader.HasRows)
                        {
                            if (expectedResults.Length != 0)
                            {
                                Console.Error.WriteLine($"Expected {expectedResults.Length} rows, got 0");
                                return false;
                            }
                            Console.WriteLine("Returns 0 rows");
                            return true;
                        }
                        if (reader.FieldCount != expectedResults[0].Length)
                        {
                            Console.Error.WriteLine($"Expected {expectedResults[0].Length} columns, got {reader.FieldCount}");
                            return false;
                        }
                        int rows = 0;
                        while (reader.Read())
                        {
                            for (int col = 0; col < expectedResults[rows].Length; col++)
                            {
                                string result = reader.GetString(col);
                                if (expectedResults[rows][col] != result)
                                {
                                    Console.Error.WriteLine($"Expected:\n'{expectedResults[rows][col]}'");
                                    Console.Error.WriteLine($"Result:\n'{result}'\nRest of the results:");
                                    while (reader.Read())
                                    {
                                        Console.Error.WriteLine(reader.GetString(0));
                                    }
                                    return false;
                                }
                            }
                            rows++;
                        }
                        Console.WriteLine("Returns " + rows + " rows");
                        if (rows != expectedResults.Length)
                        {
                            Console.Error.WriteLine($"Expected {expectedResults.Length} rows");
                            return false;
                        }
                        return true;
                    }
                }
                catch (SqlException e)
                {
                    Console.Error.WriteLine(e.Message);
                    return false;
                }
            }
        }
    }

    public static void Main(string[] args)
    {
        if (args.Length < 5)
        {
            Console.Error.WriteLine("Usage: PGTest <ip> <port> <user> <password> <testFile>");
            Environment.Exit(1);
        }

        var tests = new Tests();
        tests.Connect(args[0], int.Parse(args[1]), args[2], args[3]);
        tests.ReadTestsFromFile(args[4]);

        if (!tests.RunTests())
        {
            tests.Disconnect();
            Environment.Exit(1);
        }
        tests.Disconnect();
    }
}