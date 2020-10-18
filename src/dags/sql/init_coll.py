
sql_init_raw = """
create database if not exists test3;

CREATE TABLE if not exists test3.raw_house (
  `id` varchar(40) NOT NULL,
  `price` bigint DEFAULT NULL,
  `dates` date DEFAULT NULL,
  `postcode` varchar(8) DEFAULT NULL,
  `types` enum('D','S','T','F','O') DEFAULT NULL,
  `age` enum('Y','N') DEFAULT NULL,
  `duration` enum('F','L') DEFAULT NULL,
  `paon` varchar(60)  DEFAULT NULL,
  `saon` varchar(60)  DEFAULT NULL,
  `street` varchar(60)  DEFAULT NULL,
  `locality` varchar(60)  DEFAULT NULL,
  `town` varchar(60)  DEFAULT NULL,
  `district` varchar(60)  DEFAULT NULL,
  `county` varchar(60)  DEFAULT NULL,
  `ppd` enum('A','B') DEFAULT NULL,
  `record_status` enum('A','C','D') DEFAULT NULL,
  PRIMARY KEY `uniq_id` (`id`),
  KEY `dt` (`dates`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE if not exists test3.raw_house_nopost like test3.raw_house;

CREATE TABLE if not exists test3.`raw_crime` (
  `months` char(7) NOT NULL,
  `lsoa` char(9) DEFAULT NULL,
  `ct` int DEFAULT NULL,
  `years` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE if not exists test3.`lookup` (
  `postcode` char(7) NOT NULL,
  `oa11` char(9) DEFAULT NULL,
  `laua` char(9) DEFAULT NULL,
  `lsoa` char(9) DEFAULT NULL,
  `park` char(9) DEFAULT NULL,
  `environment` varchar(6) DEFAULT NULL,
  `isPark` tinyint(1) DEFAULT NULL,
  `country` enum('ENGLAND','WALES','SCOTLAND','NORTHERN IRELAND','CHANNEL ISLANDS','ISLE OF MAN') DEFAULT NULL,
  `sector` char(5) DEFAULT NULL,
  PRIMARY KEY (`postcode`),
  KEY `lookup2_lsoa_IDX` (`lsoa`) USING BTREE,
  KEY `lookup2_laua_IDX` (`laua`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE test3.`crime_tmp` (
  `years` int NOT NULL,
  `lsoa` char(9) NOT NULL,
  `ct` int DEFAULT '0',
  `sector` char(5) DEFAULT NULL,
  PRIMARY KEY (`lsoa`,`years`),
  KEY `crime_tmp_lsoa_IDX` (`lsoa`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS test3.`house_tmp` (
  `id` varchar(40) NOT NULL,
  `price` bigint DEFAULT NULL,
  `dates` date DEFAULT NULL,
  `postcode` varchar(8) DEFAULT NULL,
  `types` enum('D','S','T','F','O') DEFAULT NULL,
  `age` enum('Y','N') DEFAULT NULL,
  `duration` enum('F','L') DEFAULT NULL,
  `town` varchar(60) DEFAULT NULL,
  `district` varchar(60) DEFAULT NULL,
  `county` varchar(60) DEFAULT NULL,
  `sector` char(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `dt` (`dates`),
  KEY k1 (postcode)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS test3.`house_tmp_nopost` like test3.house_tmp;
"""

sql_gdp = """
alter table test3.gdp modify la_code char(9);
alter table test3.gdp modify years int(4);
create index k1 on test3.gdp(la_code);
"""

sql_separate_house = """
insert into test3.raw_house_nopost select * from test3.raw_house where postcode='';
delete from test3.raw_house where postcode='';
"""


sql_init_wh = """
create database if not exists house;

CREATE TABLE IF NOT EXISTS house.`Times` (
  `timeID` int NOT NULL AUTO_INCREMENT,
  `days` date DEFAULT NULL,
  `months` int DEFAULT NULL,
  `years` int DEFAULT NULL,
  PRIMARY KEY (`timeID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS house.`Locations` (
  `locationID` int NOT NULL AUTO_INCREMENT,
  `county` varchar(60) DEFAULT NULL,
  `district` varchar(60) DEFAULT NULL,
  `town` varchar(60) DEFAULT NULL,
  `sector` char(6) DEFAULT NULL,
  `country` enum('ENGLAND','WALES','SCOTLAND','NORTHERN IRELAND','CHANNEL ISLANDS','ISLE OF MAN') DEFAULT NULL,
  PRIMARY KEY (`locationID`),
  UNIQUE KEY `Locations_sector_IDX` (`sector`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS house.`Sales` (
  `id` varchar(40) NOT NULL,
  `timeID` int DEFAULT NULL,
  `locationID` int DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `types` enum('D','S','T','F','O') DEFAULT NULL,
  `age` enum('Y','N') DEFAULT NULL,
  `duration` enum('F','L') DEFAULT NULL,
  `isPark` tinyint(1) DEFAULT NULL,
  `environment` enum('A1','B1','C1','C2','D1','D2','E1','E2','F1','F2','1','2','3','4','5','6','7','8','Z9') DEFAULT NULL,
  `gdp` int DEFAULT NULL,
  `crime` int DEFAULT NULL,
  `postcode` varchar(8) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `timeid` (`timeID`),
  KEY `locationid` (`locationID`),
  CONSTRAINT `Sales_ibfk_1` FOREIGN KEY (`timeID`) REFERENCES `Times` (`timeID`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `Sales_ibfk_2` FOREIGN KEY (`locationID`) REFERENCES `Locations` (`locationID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS house.`Sales_old` (
  `id` varchar(40) NOT NULL,
  `timeID` int DEFAULT NULL,
  `locationID` int DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `types` enum('D','S','T','F','O') DEFAULT NULL,
  `age` enum('Y','N') DEFAULT NULL,
  `duration` enum('F','L') DEFAULT NULL,
  `isPark` tinyint(1) DEFAULT NULL,
  `environment` enum('A1','B1','C1','C2','D1','D2','E1','E2','F1','F2','1','2','3','4','5','6','7','8','Z9') DEFAULT NULL,
  `gdp` int DEFAULT NULL,
  `crime` int DEFAULT NULL,
  `postcode` varchar(8) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `timeid` (`timeID`),
  KEY `locationid` (`locationID`),
  CONSTRAINT `Sales_ibfk_3` FOREIGN KEY (`timeID`) REFERENCES `Times` (`timeID`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `Sales_ibfk_4` FOREIGN KEY (`locationID`) REFERENCES `Locations` (`locationID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

set @d = (select min(dates) from test3.raw_house);
call test3.generateTimes(@d);

"""

sql_fill_lookup = """
update test3.lookup set postcode = replace(postcode,' ','');

update test3.lookup set isPark = case when park in ('E99999999', 'W31000001', 'S99999999') then 0 else 1 end;

update test3.lookup set country = 
	case when left(oa11, 1) = 'E' then 'ENGLAND'
		when left(oa11, 1) = 'W' then 'WALES'
		when left(oa11, 1) = 'S' then 'SCOTLAND'
		when left(oa11, 1) = 'N' then 'NORTHERN IRELAND'
		when left(oa11, 1) = 'L' then 'CHANNEL ISLANDS'
		when left(oa11, 1) = 'M' then 'ISLE OF MAN' end;

update test3.lookup set sector = left(postcode, length(postcode)-2);
"""

def sql_fill_wh(year): 
  table = "Sales" if year >= 2011 else "Sales_old"
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


def sql_move_house(year): 
  return """
  truncate table test3.house_tmp; 

  insert into test3.house_tmp(id,price,dates,postcode,types,age,duration,town,district,county)
  select substring(id, 2, length(id)-2),price,dates,
    replace(postcode,' ',''),types,age,duration,town,district,county 
  from test3.raw_house
  where dates >= '{}-01-01' and dates < '{}-01-01';
  """.format(year, year+1)
