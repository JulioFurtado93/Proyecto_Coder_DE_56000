CREATE TABLE open_weather
(
	dt DATETIME,
	name VARCHAR(50),
	country_code VARCHAR(3),
	coord_lon FLOAT,
	coord_lat FLOAT,
	main VARCHAR(50),
	description VARCHAR(50),
	temperature FLOAT,
	feels_like FLOAT,
	temp_min FLOAT,
	temp_max FLOAT,
	pressure INT,
	humidity INT,
	wind_speed FLOAT,
	wind_deg INT,
	clouds INT,
	sunrise DATETIME,
	sunset DATETIME
	);