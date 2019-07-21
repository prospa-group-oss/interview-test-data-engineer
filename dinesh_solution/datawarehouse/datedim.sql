CREATE TABLE dates (
  date_id          BIGINT PRIMARY KEY,
  date             DATE NOT NULL,
  timestamp        BIGINT NOT NULL,
  weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
  day_of_week      CHAR(10) NOT NULL,
  month            CHAR(10) NOT NULL,
  month_day        INT NOT NULL,
  year             INT NOT NULL,
  week_starting_monday CHAR(2) NOT NULL,
  UNIQUE KEY `date` (`date`),
  KEY `year_week` (`year`,`week_starting_monday`)
);