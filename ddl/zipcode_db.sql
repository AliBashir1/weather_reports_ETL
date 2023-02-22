-- create index on zipcodes_info for better query performances
-- CREATE INDEX county_index on zipcodes_info(county);
-- CREATE INDEX city_index on zipcodes_info(city);
-- CREATE INDEX state_index on zipcodes_info(state);

CREATE TABLE `zipcodes_info` (
  `id` int NOT NULL AUTO_INCREMENT,
  `zipcode` int NOT NULL,
  `zipcode_type` varchar(50) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL,
  `county` varchar(50) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `country` varchar(2) DEFAULT NULL,
  `population` int DEFAULT NULL,
  `area_codes` varchar(50) DEFAULT NULL,
  `timezone` varchar(50) DEFAULT NULL,

  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=42736 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci


CREATE TABLE `weather_reports` (
  `id` int NOT NULL AUTO_INCREMENT,
  `zipcode` int NOT NULL,
  `local_time` datetime DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  `day_of_week` varchar(20) DEFAULT NULL,
  `temp_f` double(3,1) DEFAULT NULL,
  `feelslike_f` double(3,1) DEFAULT NULL,
  `condition` varchar(20) DEFAULT NULL,
  `wind_mph` double(3,1) DEFAULT NULL,
  `wind_degree` int DEFAULT NULL,
  `wind_dir` varchar(5) DEFAULT NULL,
  `pressure_mb` double(5,1) DEFAULT NULL,
  `precip_mm` double(3,1) DEFAULT NULL,
  `humidity` int DEFAULT NULL,
  `cloud` int DEFAULT NULL,
  `vis_miles` double(3,1) DEFAULT NULL,
  `uv` double(2,1) DEFAULT NULL,
  `gust_mph` double(3,1) DEFAULT NULL,

  PRIMARY KEY (`id`),
  KEY `zipcode` (`zipcode`),
  CONSTRAINT `weather_reports_ibfk_1` FOREIGN KEY (`zipcode`) REFERENCES `zipcodes_info` (`zipcode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
