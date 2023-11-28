--Import movies table
CREATE EXTERNAL TABLE movies (title string, year int, month int, genres string, duration int, countries string, rating double, votes int, budget bigint, boxoffice bigint, currency string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE LOCATION 'hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean';

--Import fx table
CREATE EXTERNAL TABLE fx (Year string, Month string, EUR string, JPY string, BGN string, CZK string, DKK string, GBP string, HUF string, PLN string, RON string, SEK string, CHF string, ISK string, NOK string, HRK string, RUB string, TRL string, TRY string, AUD string, BRL string, CAD string, CNY string, HKD string, IDR string, ILS string, INR string, KRW string, MXN string, MYR string, NZD string, PHP string, SGD string, THB string, ZAR string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE LOCATION 'hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/clean';

--Group by year and month and compute aggregate means
CREATE VIEW temp_fx AS
SELECT 
    Year, 
    CASE
        WHEN substr(Month, 1, 1) = '0' THEN substr(Month, 2)
        ELSE Month
    END as Month,
    AVG(cast(EUR as double)) as EUR,
    AVG(cast(JPY as double)) as JPY,
    AVG(cast(BGN as double)) as BGN,
    AVG(cast(CZK as double)) as CZK,
    AVG(cast(DKK as double)) as DKK,
    AVG(cast(GBP as double)) as GBP,
    AVG(cast(HUF as double)) as HUF,
    AVG(cast(PLN as double)) as PLN,
    AVG(cast(RON as double)) as RON,
    AVG(cast(SEK as double)) as SEK,
    AVG(cast(CHF as double)) as CHF,
    AVG(cast(ISK as double)) as ISK,
    AVG(cast(NOK as double)) as NOK,
    AVG(cast(HRK as double)) as HRK,
    AVG(cast(RUB as double)) as RUB,
    AVG(cast(TRL as double)) as TRL,
    AVG(cast(TRY as double)) as TRY,
    AVG(cast(AUD as double)) as AUD,
    AVG(cast(BRL as double)) as BRL,
    AVG(cast(CAD as double)) as CAD,
    AVG(cast(CNY as double)) as CNY,
    AVG(cast(HKD as double)) as HKD,
    AVG(cast(IDR as double)) as IDR,
    AVG(cast(ILS as double)) as ILS,
    AVG(cast(INR as double)) as INR,
    AVG(cast(KRW as double)) as KRW,
    AVG(cast(MXN as double)) as MXN,
    AVG(cast(MYR as double)) as MYR,
    AVG(cast(NZD as double)) as NZD,
    AVG(cast(PHP as double)) as PHP,
    AVG(cast(SGD as double)) as SGD,
    AVG(cast(THB as double)) as THB,
    AVG(cast(ZAR as double)) as ZAR
FROM 
    fx
GROUP BY 
    Year, Month;

INSERT OVERWRITE TABLE fx
SELECT * FROM temp_fx;

DROP VIEW temp_fx;
