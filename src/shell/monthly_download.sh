#!/bin/bash

root=$(dirname `realpath $0`)
dir="$root/../../data"

cd $dir

# sales data
wget http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv -O new_ppd.csv

# crime
wget https://data.police.uk/data/archive/latest.zip 
unzip latest.zip -d latest

# gdp
# yearly?


