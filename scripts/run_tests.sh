#!/bin/bash

echo "Running all unittest test files..."

# Navigate to the internship directory
cd ..

cd test

echo "Running test - prepare tables"
python3 test_prepare_tables.py


echo "Running test - parse json"
python3 test_parse_json.py

echo "Running test - force drain"
python3 test_force_drain.py


echo "Running test - timestamp filtering"
python3 test_timestamp_filtering.py

echo "Running test - write to spanner"
python3 test_write_to_spanner.py



echo "All tests executed."
