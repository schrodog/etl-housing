drop procedure if exists test3.generateTimes;

DELIMITER //
CREATE PROCEDURE test3.generateTimes(x date)
BEGIN
	while x <= CURDATE() do 
		insert into house.Times(days,months,years) values(x, month(x), year(x));
		set x = date_add(x, interval 1 day);
	END while;
END// 
DELIMITER ;
