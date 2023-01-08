# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.

# Extract phase

echo "Extracting data"

# Download the data from the web
curl -O "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"

# unpack the file
gzip -d web-server-access-log.txt.gz

# Transform phase
echo "Transforming data"

# Remove the trails info
cut -d"#" -f1-4 web-server-access-log.txt > extracted-data-log.txt

# read the extracted data and replace the # with commas.
tr "#" "," < extracted-data-log.txt > transformed-data-log.csv

# Load phase
echo "Loading data"
# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.

echo "\c template1;\COPY access_log  FROM '/home/project/transformed-data-log.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost