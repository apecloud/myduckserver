require 'mysql2'

class MySQLTest
  class Tests
    def initialize
      @conn = nil
      @tests = []
    end

    def connect(ip, port, user, password)
      begin
        @conn = Mysql2::Client.new(host: ip, port: port, username: user, password: password)
      rescue Mysql2::Error => e
        raise "Connection failed: #{e.message}"
      end
    end

    def disconnect
      @conn.close if @conn
    end

    def add_test(query, expected_results)
      @tests << Test.new(query, expected_results)
    end

    def run_tests
      @tests.each do |test|
        return false unless test.run(@conn)
      end
      true
    end

    def read_tests_from_file(filename)
      begin
        File.open(filename, 'r') do |file|
          query = nil
          results = []
          file.each_line do |line|
            line = line.strip
            if line.empty?
              if query
                add_test(query, results)
                query = nil
                results = []
              end
            else
              if query.nil?
                query = line
              else
                results << line.split(',')
              end
            end
          end
          add_test(query, results) if query
        end
      rescue IOError => e
        puts "File read error: #{e.message}"
        exit 1
      end
    end

    class Test
      def initialize(query, expected_results)
        @query = query
        @expected_results = expected_results
      end

      def run(conn)
        begin
          puts "Running test: #{@query}"
          result = conn.query(@query)
          if result.nil? || result.count == 0
            if @expected_results.empty?
              puts "Returns 0 rows"
              return true
            end
            puts "Expected #{@expected_results.length} rows, got 0"
            return false
          end
          if result.fields.size != @expected_results[0].length
            puts "Expected #{@expected_results[0].length} columns, got #{result.fields.size}"
            return false
          end
          result.each_with_index do |row, row_num|
            row.each_with_index do |(col_name, value), col_num|
              expected = @expected_results[row_num][col_num]
              if value.to_s != expected.to_s
                puts "Expected:\n'#{expected}'"
                puts "Result:\n'#{value}'\nRest of the results:"
                result.each { |r| puts r.values.join(',') }
                return false
              end
            end
          end
          puts "Returns #{result.count} rows"
          if result.count != @expected_results.length
            puts "Expected #{@expected_results.length} rows"
            return false
          end
          true
        rescue Mysql2::Error => e
          puts e.message
          false
        end
      end
    end
  end

  def self.main(args)
    if args.length < 5
      puts "Usage: ruby MySQLTest.rb <ip> <port> <user> <password> <testFile>"
      exit 1
    end

    tests = Tests.new
    tests.connect(args[0], args[1].to_i, args[2], args[3])
    tests.read_tests_from_file(args[4])

    unless tests.run_tests
      tests.disconnect
      exit 1
    end
    tests.disconnect
  end
end

if __FILE__ == $0
  MySQLTest.main(ARGV)
end