DROP KEYSPACE IF EXISTS shop_rf3;

CREATE KEYSPACE shop_rf3 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE shop_rf3;

CREATE TABLE likes_counter (
    id int PRIMARY KEY,
    likes counter
);

SELECT * FROM likes_counter;