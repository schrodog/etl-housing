
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
  `timeID` int DEFAULT NULL,
  `locationID` int DEFAULT NULL,
  `environment` varchar(6) DEFAULT NULL,
  `isPark` tinyint(1) DEFAULT NULL,
  `country` enum('ENGLAND','WALES','SCOTLAND','NORTHERN IRELAND','CHANNEL ISLANDS','ISLE OF MAN') DEFAULT NULL,
   `crime` int DEFAULT '-1',
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
  PRIMARY KEY (`id`),
  KEY `timeid` (`timeID`),
  KEY `locationid` (`locationID`),
  CONSTRAINT `Sales_ibfk_3` FOREIGN KEY (`timeID`) REFERENCES `Times` (`timeID`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `Sales_ibfk_4` FOREIGN KEY (`locationID`) REFERENCES `Locations` (`locationID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




"""

sql_fill_lookup = """
update test3.lookup set isPark = case when park in ('E99999999', 'W31000001', 'S99999999') then 0 else 1 end;

update test3.lookup set country = 
	case when left(oa11, 1) = 'E' then 'ENGLAND'
		when left(oa11, 1) = 'W' then 'WALES'
		when left(oa11, 1) = 'S' then 'SCOTLAND'
		when left(oa11, 1) = 'N' then 'NORTHERN IRELAND'
		when left(oa11, 1) = 'L' then 'CHANNEL ISLANDS'
		when left(oa11, 1) = 'M' then 'ISLE OF MAN' end;

"""

sql_fill_wh = """
set @d = (select min(dates) from test3.raw_house);
call test3.generateTimes(@d);

insert into test3.house_tmp_nopost select * from test3.house_tmp where postcode='';
delete from test3.house_tmp where postcode='';
update test3.house_tmp set sector = left(postcode, length(postcode)-2);

update test3.lookup set postcode = replace(postcode,' ','');

insert ignore into house.Locations (county,district,town,sector)
SELECT county,district,town,sector
from test3.house_tmp h 
group by county,district,town,sector;

update test3.house_tmp a, test3.crime_tmp b, test3.lookup c set a.crime = b.ct 
where year(a.dates) = b.years and a.postcode = c.postcode and c.lsoa = b.lsoa ;

"""


def sql_move_house(year): 
  return """
  insert into test3.house_tmp(id,price,dates,postcode,types,age,duration,town,district,county)
  select substring(id, 2, length(id)-2),price,dates,
    replace(postcode,' ',''),types,age,duration,town,district,county 
  from test3.raw_house
  where dates >= '{}-01-01' and dates < '{}-01-01'
  """.format(year, year+1)
