# Generated by ChatGPT

import re
import sys

def extract_subtest_names(test_suite, error_log_file):
    # Read the error log from the specified file
    with open(error_log_file, "r") as file:
        error_log = file.read()

    # Regex pattern to match the subtest names for the given test suite and extract only the part after the last '/'
    pattern = rf'--- FAIL: {test_suite}/(?:[^\s/]*/)*([^\s/]+)'

    # Extract all matches
    matches = re.findall(pattern, error_log)

    # Generate Go list format
    if matches:
        go_list = '[]string{\n'
        for match in matches:
            # Replace double quotes with escaped double quotes
            formatted_match = match.replace('"', r'\"')
            go_list += f'\t"{formatted_match}",\n'
        go_list += '}'
        print(go_list)
    else:
        print(f"No subtest names found for {test_suite} in the log file.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <TestSuiteName> <ErrorLogFile>")
        print("\nExample: python extract_queries_to_skip.py TestQueriesSimple ./errors.txt")
    else:
        test_suite_name = sys.argv[1]
        error_log_filename = sys.argv[2]
        extract_subtest_names(test_suite_name, error_log_filename)
