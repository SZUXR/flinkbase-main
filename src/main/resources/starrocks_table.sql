-- StarRocks建表语句
CREATE DATABASE IF NOT EXISTS test1;

USE test1;

CREATE TABLE IF NOT EXISTS user_behavior_result (
    userId BIGINT,
    itemId VARCHAR(50),
    timestamp BIGINT,
    categoryId VARCHAR(50),
    behavior VARCHAR(20),
    date VARCHAR(10),
    hour INT,
    behaviorCount BIGINT
) ENGINE=OLAP
DUPLICATE KEY(userId, itemId, timestamp)
DISTRIBUTED BY HASH(userId) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);