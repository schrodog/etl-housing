def sql_fill_wh(old=True): 
  table = "Sales_old" if old else "Sales"
  return """
    insert ignore into test3.house_tmp_nopost select * from test3.house_tmp where postcode='';
    delete from test3.house_tmp where postcode='';
    update test3.house_tmp set sector = left(postcode, length(postcode)-2);

    insert ignore into house.Locations (county,district,town,sector)
    SELECT county,district,town,sector
    from test3.house_tmp h 
    group by county,district,town,sector;

    insert ignore into house.{}(id,timeID,locationID,price,types,age,duration,isPark,environment,gdp,crime,postcode)
    select h.id, t1.timeID,l2.locationID ,h.price,h.types,h.age,h.duration,l.isPark,l.environment,g.gdp,ct.ct,h.postcode
    from test3.house_tmp h
      left join test3.lookup l on h.postcode = l.postcode 
      left join test3.gdp g on year(h.dates) = g.years and l.laua = g.la_code 
      left join crime_tmp ct on year(h.dates) = ct.years and l.lsoa = ct.lsoa 
      left join house.Times t1 on t1.days = h.dates
      left join house.Locations l2 on l2.sector = h.sector ;
    """.format(table)


def sql_move_house(old=True): 
  op = "<" if old else ">="
  return """
  truncate table test3.house_tmp; 

  insert into test3.house_tmp(id,price,dates,postcode,types,age,duration,town,district,county)
  select substring(id, 2, length(id)-2),price,dates,
    replace(postcode,' ',''),types,age,duration,town,district,county 
  from test3.raw_house
  where dates {} '2011-01-01';
  """.format(op)


sql_init_agg = """
CREATE TABLE house.`Monthly_sales` (
  `id` int(11) auto_increment,
  `year` int DEFAULT NULL,
  `month` int default null,
  `locationID` int DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `age` float DEFAULT NULL,
  `duration` float DEFAULT NULL,
  `isPark` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `locationid` (`locationID`),
  CONSTRAINT `Sales_ibfk_5` FOREIGN KEY (`locationID`) REFERENCES `Locations` (`locationID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE house.`Yearly_sales` (
  `id` int(11) auto_increment,
  `year` int DEFAULT NULL,
  `locationID` int DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `age` float DEFAULT NULL,
  `duration` float DEFAULT NULL,
  `isPark` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `locationid` (`locationID`),
  CONSTRAINT `Sales_ibfk_6` FOREIGN KEY (`locationID`) REFERENCES `Locations` (`locationID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

"""

sql_update_agg = """
insert into house.Monthly_sales(year,month,locationID,price,age,duration,isPark)
select 
	max(t.years), t.months, s.locationID, avg(price), 
		sum(case when age='Y' then 1 else 0 end)/COUNT(*),
		sum(case when duration='F' then 1 else 0 end)/COUNT(*),
		sum(isPark)/COUNT(*)
from house.Sales s 
	left join house.Times t on s.timeID = t.timeID
group by t.months, s.locationID ;

insert into house.Yearly_sales(year,locationID,price,age,duration,isPark)
select 
	t.years, s.locationID, avg(price), 
		sum(case when age='Y' then 1 else 0 end)/COUNT(*),
		sum(case when duration='F' then 1 else 0 end)/COUNT(*),
		sum(isPark)/COUNT(*)
from house.Sales s 
	left join house.Times t on s.timeID = t.timeID
group by t.years, s.locationID ;
"""

