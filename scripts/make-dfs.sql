CREATE TABLE dataset1 AS 
SELECT 
    ROW_NUMBER() OVER () as id,
    RANDOM() * 100 as value1,
    CASE WHEN RANDOM() < 0.5 THEN 'A' ELSE 'B' END as category,
    DATE '2023-01-01' + INTERVAL (RANDOM() * 365) DAY as date
FROM range(1000);

-- Create the second random dataset
CREATE TABLE dataset2 AS 
SELECT 
    ROW_NUMBER() OVER () as id,
    RANDOM() * 1000 as value2,
    ARRAY['X', 'Y', 'Z'][CAST(RANDOM() * 3 + 1 AS INTEGER)] as group,
    TIMESTAMP '2023-01-01 00:00:00' + INTERVAL (RANDOM() * 31536000) SECOND as timestamp
FROM range(1500);

-- Save datasets as Parquet files
COPY dataset1 TO 'data/df.parquet' (FORMAT PARQUET);
COPY dataset2 TO 'data/subfolder/df-2.parquet' (FORMAT PARQUET);

-- Verify the files were created (this part can't be done in pure SQL, 
-- you'll need to check the files manually or use a shell command)

-- Optionally, you can query the tables to see their contents
SELECT * FROM 'data/df.parquet' LIMIT 5;
SELECT * FROM 'data/subfolder/df-2.parquet' LIMIT 5;

