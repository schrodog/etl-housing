#!/bin/bash

root=$(dirname `realpath $0`)
dir="$root/../../data"

# lookup data
wget https://www.arcgis.com/sharing/rest/content/items/ad7fd1d95f06431aaaceecdce4985c7e/data -O $dir/lookup.zip
cd $dir
unzip lookup.zip -d lookup

# gdp
wget https://www.ons.gov.uk/file?uri=%2feconomy%2fgrossdomesticproductgdp%2fdatasets%2fregionalgrossdomesticproductlocalauthorities%2f1998to2018/regionalgrossdomesticproductgdplocalauthorities.xlsx -O gdp.xlsx

# crime
# 12/2010 to 12/2016
wget https://data.police.uk/data/archive/2016-12.zip | unzip -d 2016-12

# 01/2017 to 12/2019
wget https://data.police.uk/data/archive/2019-12.zip | unzip -d 2019-12

# 09/2017 to 08/2020
wget https://data.police.uk/data/archive/2020-08.zip | unzip -d 2020-08

# postcode lookup
wget http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update.txt -O $root/../../data/lookup.zip


