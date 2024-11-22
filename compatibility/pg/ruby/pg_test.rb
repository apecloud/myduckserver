require 'pg'

class PGTest
  class Tests
    def initialize
      @conn = nil
      @tests = []
    end

    def connect(ip, port, user, password)
      begin
        @conn = PG.connect(host: ip, port: port, user: user, password: password, dbname: 'main')
      rescue PG::Error => e
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
          result = conn.exec(@query)
          if result.ntuples == 0
            puts "Returns 0 rows"
            return @expected_results.empty?
          end
          if result.nfields != @expected_results[0].length
            puts "Expected #{@expected_results[0].length} columns, got #{result.nfields}"
            return false
          end
          result.each_with_index do |row, row_num|
            row.each_with_index do |(col_name, value), col_num|
              expected = @expected_results[row_num][col_num]
              if value != expected
                puts "Expected:\n'#{expected}'"
                puts "Result:\n'#{value}'\nRest of the results:"
                result.each_row { |r| puts r.join(',') }
                return false
              end
            end
          end
          puts "Returns #{result.ntuples} rows"
          if result.ntuples != @expected_results.length
            puts "Expected #{@expected_results.length} rows"
            return false
          end
          true
        rescue PG::Error => e
          puts e.message
          false
        end
      end
    end
  end

  def self.main(args)
    if args.length < 5
      puts "Usage: ruby PGTest.rb <ip> <port> <user> <password> <testFile>"
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
  PGTest.main(ARGV)
end