#! /bin/bash

# Extract readings with get_temp_API

# Append reading to temperature.log
get_temp_API >> temperature.log

# Buffer last hour of readings
tail -60 temperature.log > temperature_buffer.log

# Call get_stats.py to aggregate the readings
python get_stats.py temperature.log temp_stats.csv

# Load the stats using load_stats_api
load_stats_api temp_stats.csv
