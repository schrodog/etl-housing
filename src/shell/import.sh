#!/bin/bash

root=$(dirname `realpath $0`)
dir="$root/../../data"
sqldir="$root/../sql"

# import housing sales
mysqlsh -h 127.0.0.1 -uroot -proot -P 3307 -e "util.importTable('$dir/pp-complete.csv',{schema:'test3', table: 'raw_house', columns: ['id','price','dates','postcode','types','age','duration','paon','saon','street','locality','town','district','county','ppd','record_status'], dialect:'csv-unix', skipRows: 0, showProgress: true, fieldsOptionallyEnclosed: true, fieldsTerminatedBy: ',', linesTerminatedBy: '\n',fieldsEnclosedBy: '\"',threads: 6, bytesPerChunk: '1G', maxRate: '2G'});"

echo "import housing sales"

# import postcode lookup table
lookupdir="$dir/lookup/multi_csv"

for file in $lookupdir/*.csv; do
  sql="load data local infile '$file' into table lookup fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines (postcode,@d,@d,@d,@d,@d,@d,@d,@d,oa11,@d,@d,laua,@d,@d,@d,@d,@d,@d,@d,@d,@d,@d,@d,park,lsoa,@d,@d,@d,@d,@d,environment)"

  mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 --local-infile=1 -e "$sql"
  echo $file
done

echo "import lookup"

# import crime data
crimedir="$dir/2016-12"
crimedir2="$dir/2019-12"

sql2="insert ignore into crime_tmp (ct,lsoa,years) select count(*) ct, lsoa, left(max(months),4) from test3.raw_crime rc where lsoa != '' group by lsoa; truncate table raw_crime;"

for year in {2011..2016}; do
  for dirs in $crimedir/$year*; do
    for file in $dirs/*.csv; do
      sql="load data local infile '$file' into table raw_crime fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines (@d,months,@d,@d,@d,@d,@d,lsoa)"
      mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 --local-infile=1 -e "$sql"
      echo $file
    done
  done
  mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 -e "$sql2"
done

for year in {2017..2019}; do
  for dirs in $crimedir2/$year*; do
    for file in $dirs/*.csv; do
      sql="load data local infile '$file' into table raw_crime fields terminated by ',' enclosed by '\"' lines terminated by '\n' ignore 1 lines (@d,months,@d,@d,@d,@d,@d,lsoa)"
      mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 --local-infile=1 -e "$sql"
      echo $file
    done
  done
  mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 -e "$sql2"
done

echo "import crime data"

mysql -B test3 -h 127.0.0.1 -uroot -proot -P3307 < "$sqldir/create_schema.sql"



