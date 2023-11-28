CREATE EXTERNAL TABLE movies (title string, year int, month int, duration int, genres string, countries string, rating double, votes int, budget bigint, boxoffice bigint, currency string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE LOCATION 'hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean';

CREATE EXTERNAL TABLE fx (Year string, Month string, EUR string, JPY string, BGN string, CZK string, DKK string, GBP string, HUF string, PLN string, RON string, SEK string, CHF string, ISK string, NOK string, HRK string, RUB string, TRL string, TRY string, AUD string, BRL string, CAD string, CNY string, HKD string, IDR string, ILS string, INR string, KRW string, MXN string, MYR string, NZD string, PHP string, SGD string, THB string, ZAR string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE LOCATION 'hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/clean';
