INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/bay2006_nyu_edu/proj/joined-quotes' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
    SELECT 
        CONCAT('"', m.title, '"') AS title,
        CONCAT('"', m.year, '"') AS year,
        CONCAT('"', m.month, '"') AS month,
        CONCAT('"', m.duration, '"') AS duration,
        CONCAT('"', m.genres, '"') AS genres,
        CONCAT('"', m.countries, '"') AS countries,
        CONCAT('"', m.rating, '"') AS rating,
        CONCAT('"', m.votes, '"') AS votes,
        CONCAT('"', m.budget, '"') AS budget,
        CONCAT('"', m.boxoffice, '"') AS boxoffice,
        CONCAT('"', m.currency, '"') AS currency,
    CASE
        WHEN UPPER(m.currency) = 'EUR' THEN f.eur
        WHEN UPPER(m.currency) = 'JPY' THEN f.jpy
        WHEN UPPER(m.currency) = 'BGN' THEN f.bgn
        WHEN UPPER(m.currency) = 'CZK' THEN f.czk
        WHEN UPPER(m.currency) = 'DKK' THEN f.dkk
        WHEN UPPER(m.currency) = 'GBP' THEN f.gbp
        WHEN UPPER(m.currency) = 'HUF' THEN f.huf
        WHEN UPPER(m.currency) = 'PLN' THEN f.pln
        WHEN UPPER(m.currency) = 'RON' THEN f.ron
        WHEN UPPER(m.currency) = 'SEK' THEN f.sek
        WHEN UPPER(m.currency) = 'CHF' THEN f.chf
        WHEN UPPER(m.currency) = 'ISK' THEN f.isk
        WHEN UPPER(m.currency) = 'NOK' THEN f.nok
        WHEN UPPER(m.currency) = 'HRK' THEN f.hrk
        WHEN UPPER(m.currency) = 'RUB' THEN f.rub
        WHEN UPPER(m.currency) = 'TRL' THEN f.trl
        WHEN UPPER(m.currency) = 'TRY' THEN f.try
        WHEN UPPER(m.currency) = 'AUD' THEN f.aud
        WHEN UPPER(m.currency) = 'BRL' THEN f.brl
        WHEN UPPER(m.currency) = 'CAD' THEN f.cad
        WHEN UPPER(m.currency) = 'CNY' THEN f.cny
        WHEN UPPER(m.currency) = 'HKD' THEN f.hkd
        WHEN UPPER(m.currency) = 'IDR' THEN f.idr
        WHEN UPPER(m.currency) = 'ILS' THEN f.ils
        WHEN UPPER(m.currency) = 'INR' THEN f.inr
        WHEN UPPER(m.currency) = 'KRW' THEN f.krw
        WHEN UPPER(m.currency) = 'MXN' THEN f.mxn
        WHEN UPPER(m.currency) = 'MYR' THEN f.myr
        WHEN UPPER(m.currency) = 'NZD' THEN f.nzd
        WHEN UPPER(m.currency) = 'PHP' THEN f.php
        WHEN UPPER(m.currency) = 'SGD' THEN f.sgd
        WHEN UPPER(m.currency) = 'THB' THEN f.thb
        WHEN UPPER(m.currency) = 'ZAR' THEN f.zar
        ELSE NULL
    END AS exchange_rate
FROM
    movies m
LEFT JOIN
    fx f ON m.year = f.year AND m.month = f.month;
