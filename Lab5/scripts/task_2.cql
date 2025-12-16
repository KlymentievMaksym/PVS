CREATE KEYSPACE shop_rf1 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE shop_rf1.items (id int PRIMARY KEY, name text);

CREATE KEYSPACE shop_rf2 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
CREATE TABLE shop_rf2.items (id int PRIMARY KEY, name text);

CREATE KEYSPACE shop_rf3 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE shop_rf3.items (id int PRIMARY KEY, name text);

INSERT INTO shop_rf1.items (id, name) VALUES (1, 'Item One');
INSERT INTO shop_rf2.items (id, name) VALUES (1, 'Item One');
INSERT INTO shop_rf3.items (id, name) VALUES (1, 'Item One');
SELECT * FROM shop_rf1.items;
SELECT * FROM shop_rf2.items;
SELECT * FROM shop_rf3.items;