-- Data Collection

# Create tables
CREATE TABLE userbehavior(
	user_id VARCHAR(20),
    item_id VARCHAR(20),
    category_id VARCHAR(20),
    behavior_type VARCHAR(20),
    time_stamp VARCHAR(20)
);

# LOAD data through MySQL command line
SET GLOBAL local_infile = on;

LOAD DATA LOCAL INFILE '/Users/hanzi/Desktop/UserBehavior.csv'
INTO TABLE userbehavior
FIELDS TERMINATED BY ',';


-- Data Cleaning

# Adding datetime column
ALTER TABLE userbehavior ADD datetime TIMESTAMP(0);

# Convert unix timestamp to datetime
UPDATE userbehavior
SET datetime = from_unixtime(time_stamp,'%Y-%m-%d %H:%i:%s');

# Adding Date column
ALTER TABLE userbehavior ADD date DATE;
UPDATE userbehavior 
SET date = DATE(userbehavior.datetime);

# Adding Hour column
ALTER TABLE userbehavior ADD hour INT;
UPDATE userbehavior
SET hour = EXTRACT(HOUR FROM DATETIME);

ALTER TABLE userbehavior DROP COLUMN weekday;

# Adding weekday column
ALTER TABLE userbehavior ADD weekday VARCHAR(10);
UPDATE userbehavior 
SET weekday = DAYOFWEEK(datetime);

# Check for duplicates
SELECT user_id,item_id,category_id,behavior_type,time_stamp,COUNT(*) AS records
FROM userbehavior
GROUP BY user_id,item_id,category_id,behavior_type,time_stamp
HAVING count(*) > 1;

# Check for null values
SELECT * 
FROM userbehavior
WHERE user_id IS NULL OR item_id IS NULL OR category_id IS NULL OR behavior_type IS NULL OR time_stamp IS NULL;

# Delete out of range records
DELETE FROM userbehavior
WHERE date < '2017-11-25' OR date > '2017-12-3';


-- Exploratory Data Analysis
# User behavior trend in a day
SELECT hour, 
	SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS fav,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS buy
FROM userbehavior
GROUP BY hour
ORDER BY hour;

# User behavior trend from 25th November to 3rd December
SELECT date, COUNT(DISTINCT user_id) AS users,
	SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS fav,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS buy
FROM userbehavior
GROUP BY date
ORDER BY date;

# Day 1 retention
SELECT u1.date, COUNT(IF(DATEDIFF(u2.date,u1.date)=1,u2.user_id,NULL)) /COUNT(IF(DATEDIFF(u2.date,u1.date)=0,u2.user_id,NULL)) AS day_1_retention
FROM 
(SELECT user_id,date 
FROM userbehavior
GROUP BY user_id,date) u1,
(SELECT user_id,date 
FROM userbehavior
GROUP BY user_id,date) u2
WHERE u1.user_id = u2.user_id AND u1.date <= u2.date
GROUP BY u1.date;       

# AIPL User Life Cycle
# Create view
CREATE VIEW `behavior` AS
SELECT user_id,datetime,date,weekday,hour,
	CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END AS pv,
	CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END AS fav,
    CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END AS cart,
    CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END AS buy
FROM userbehavior;

# Calculate awareness, interest, and purchase
SELECT u1.awareness,u1.interest,u1.purchase,u2.loyalty,
	u1.interest/u1.awareness AS A_to_I_conversion,
	u1.purchase/u1.interest AS I_to_P_conversion,
    u2.loyalty/u1.purchase AS P_to_L_conversion
FROM
(SELECT SUM(pv) AS awareness, SUM(fav)+SUM(cart) AS interest, SUM(buy) AS purchase
FROM behavior) u1
JOIN 
(SELECT SUM(buy) AS loyalty
FROM(
SELECT user_id,datetime,buy,DENSE_RANK() OVER(PARTITION BY user_id ORDER BY datetime) AS n_purchase
FROM behavior
WHERE buy = 1
ORDER BY user_id,datetime) AS subquery
WHERE n_purchase > 1) u2;
-- Conclusion: awareness to interest conversion is low (9.33%) and needs to be improved

# A-I conversion break down by hour
SELECT hour, SUM(pv) AS awareness, SUM(cart)+SUM(fav) AS interest,(SUM(cart)+SUM(fav))/SUM(pv) AS conversion
FROM behavior
GROUP BY hour
ORDER BY conversion DESC;

# A-I conversion break down by category
WITH cte AS(
SELECT category_id,SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS awareness,
	SUM(CASE WHEN behavior_type = 'fav' OR behavior_type = 'cart' THEN 1 ELSE 0 END) AS interest
FROM userbehavior
GROUP BY category_id)
SELECT category_id,interest/awareness AS conversion
FROM cte
WHERE interest/awareness < 1 AND awareness != 0
ORDER BY conversion DESC;

# Purchase Frequency
CREATE VIEW consume_dates AS
SELECT DISTINCT user_id, date, DENSE_RANK() OVER(PARTITION BY user_id ORDER BY date) AS n_purchase
FROM userbehavior
WHERE behavior_type = 'buy';

# Average purchase frequency
WITH cte AS(
SELECT c2.user_id, c2.date, c2.n_purchase,
	DATEDIFF(c2.date,c1.date) AS purchase_frequency
FROM consume_dates c1,consume_dates c2
WHERE c1.user_id = c2.user_id 
AND c1.n_purchase = c2.n_purchase - 1)
SELECT AVG(purchase_frequency) AS avg_purchase_frequency
FROM cte;

# Improve I-P rate using RFM(Recency,Frequency,Monetary)
-- Redefine RFM:
-- R: How recent was the user's last purchase?
-- F: Number of 'fav' and 'cart'
-- M: Number of 'buy'
CREATE VIEW RFM AS
SELECT a.user_id,a.R,b.F,b.M
FROM(
SELECT user_id,MAX(date),DATEDIFF('2017-12-03',MAX(date)) AS R
FROM consume_dates 
GROUP BY user_id) a
LEFT JOIN(
SELECT user_id,SUM(fav)+SUM(cart) AS F,SUM(buy) AS M
FROM behavior
GROUP BY user_id) b
ON a.user_id = b.user_id;

# Calculate threshold for RFM using averages
SELECT AVG(R),AVG(F),AVG(M)
FROM RFM;

# Create user segments using RFM
-- CREATE VIEW RFM_segments AS
CREATE VIEW RFM_segments AS
SELECT user_id,
	CASE WHEN R < 2.5241 AND F > 9.485 AND M > 3.0437 THEN 'champion'
    WHEN R < 2.5241 AND F > 9.485 AND M < 3.0437 THEN 'loyal customers'
    WHEN R < 2.5241 AND F < 9.485 AND M > 3.0437 THEN 'potential loyalists'
    WHEN R < 2.5241 AND F < 9.485 AND M < 3.0437 THEN 'new customer'
    WHEN R > 2.5241 AND F > 9.485 AND M > 3.0437 THEN "can't lose them"
    WHEN R > 2.5241 AND F > 9.485 AND M < 3.0437 THEN 'needs attention'
    WHEN R > 2.5241 AND F < 9.485 AND M > 3.0437 THEN 'at risk'
    WHEN R > 2.5241 AND F < 9.485 AND M < 3.0437 THEN 'hibernating'
    END AS 'segments'
FROM RFM;

# Number of users in each segment
SELECT segments, COUNT(user_id) AS users, ROUND(COUNT(user_id)/ SUM(COUNT(user_id)) OVER(),2) AS percentage
FROM RFM_segments
GROUP BY segments
ORDER BY percentage DESC;

# Purchase journey 
CREATE TEMPORARY TABLE journey_type_info(
	type VARCHAR(10),
    description VARCHAR(30));

INSERT INTO journey_type_info 
VALUES('0001','buy directly'),
	('1001','buy after view'),
    ('0101','buy after fav'),
    ('0011','buy after cart'),
    ('1101','buy after view&fav'),
    ('1011','buy after view&cart'),
    ('0111','buy after fav&cart'),
    ('1111','buy after view&fav&cart');

CREATE VIEW user_behavior_view AS
SELECT user_id,item_id
,COUNT(IF(behavior_type='pv',behavior_type,null)) 'pv'
,COUNT(IF(behavior_type='fav',behavior_type,null)) 'fav'
,COUNT(IF(behavior_type='cart',behavior_type,null)) 'cart'
,COUNT(IF(behavior_type='buy',behavior_type,null)) 'buy'
FROM userbehavior
GROUP BY user_id,item_id;

CREATE VIEW user_behavior_standard AS
SELECT user_id,item_id,
	(CASE WHEN pv>0 then 1 else 0 end) pv,
	(CASE WHEN fav>0 then 1 else 0 end) fav,
	(CASE WHEN cart>0 THEN 1 else 0 end) cart,
	(CASE WHEN buy>0 THEN 1 else 0 end) buy
from user_behavior_view;

CREATE VIEW journey_type AS
SELECT *,
CONCAT(pv,fav,cart,buy) AS purchase_journey
FROM user_behavior_standard 
where buy > 0;

WITH cte AS(
SELECT purchase_journey, COUNT(*) AS num
FROM journey_type
GROUP BY purchase_journey)
SELECT c.purchase_journey, j.description, c.num
FROM cte c
LEFT JOIN journey_type_info j
ON c.purchase_journey = j.type;

# Popular items
# Top 10 popular items by pv
SELECT item_id, COUNT(*) AS num
FROM userbehavior
WHERE behavior_type = 'pv'
GROUP BY item_id
ORDER BY num DESC
LIMIT 10;

# Top 10 popular items by fav
SELECT item_id, COUNT(*) AS num
FROM userbehavior
WHERE behavior_type = 'fav'
GROUP BY item_id
ORDER BY num DESC
LIMIT 10;

# Top 10 popular items by cart
SELECT item_id, COUNT(*) AS num
FROM userbehavior
WHERE behavior_type = 'cart'
GROUP BY item_id
ORDER BY num DESC
LIMIT 10;

# Top 10 popular items by buy
SELECT item_id, COUNT(*) AS num
FROM userbehavior
WHERE behavior_type = 'buy'
GROUP BY item_id
ORDER BY num DESC
LIMIT 10;