#!/bin/bash

root=$(dirname `realpath $0`)
dir="$root/../../data"
sqldir="$root/../sql"

# ppd

sql="create table if not exists test3.raw_latest_house like test3.raw_house"

mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 -e "$sql"

file="$dir/new_ppd.csv"

import_sql="load data local infile '$file' into table raw_latest_house fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines (id,price,dates,postcode,types,age,duration,paon,saon,street,locality,town,district,county,ppd,record_status)"

mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 -e "$import_sql"


# crime
cd "$dir/latest"
maxfolder=`ls | sort -rn | head -n 1`

crimedir="$dir/latest/$maxfolder"

sql3="insert ignore into crime_tmp (ct,lsoa,years) select count(*)/8.0 ct, lsoa, left(max(months),4) from test3.raw_crime rc where lsoa != '' group by lsoa; truncate table raw_crime; "

for file in $crimedir/*.csv; do
  sql="load data local infile '$file' into table test3,raw_crime fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines (@d,months,@d,@d,@d,@d,@d,lsoa)"
  mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 --local-infile=1 -e "$sql"
  echo $file
done
mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 -e "$sql3"






