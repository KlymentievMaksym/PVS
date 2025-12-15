/* 1. СТВОРЕННЯ KEYSPACE */
CREATE KEYSPACE IF NOT EXISTS shop 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE shop;

/* 2. ТАБЛИЦЯ ITEMS */
CREATE TABLE IF NOT EXISTS items (
    category text,
    price decimal,
    id uuid,
    name text,
    producer text,
    attributes map<text, text>,
    PRIMARY KEY ((category), price, id)
) WITH CLUSTERING ORDER BY (price ASC);

CREATE INDEX IF NOT EXISTS items_attributes_idx ON items(ENTRIES(attributes));
CREATE INDEX IF NOT EXISTS items_name_idx ON items(name);

/* Materialized View для пошуку за ціною та виробником */
CREATE MATERIALIZED VIEW IF NOT EXISTS items_by_producer AS
SELECT category, producer, price, id, name, attributes
FROM items
WHERE category IS NOT NULL AND producer IS NOT NULL AND price IS NOT NULL AND id IS NOT NULL
PRIMARY KEY ((category), producer, price, id);

/* Наповнення ITEMS */
/* === КАТЕГОРІЯ: КОНСТРУКТОРИ (Constructors) === */
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 450, uuid(), 'Classic Creative Bricks', 'LEGO', {'pieces': '200', 'age': '4+', 'material': 'plastic'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 899, uuid(), 'Technic Race Car', 'LEGO', {'pieces': '450', 'age': '9+', 'difficulty': 'medium'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 3200, uuid(), 'Star Wars Millennium Falcon', 'LEGO', {'pieces': '1351', 'age': '12+', 'theme': 'Star Wars'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 1500, uuid(), 'City Police Station', 'LEGO', {'pieces': '800', 'age': '6+', 'theme': 'City'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 650, uuid(), 'Minecraft The Cave', 'LEGO', {'pieces': '300', 'age': '8+', 'theme': 'Minecraft'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 300, uuid(), 'Tank T-34', 'Sluban', {'pieces': '150', 'age': '6+', 'origin': 'China'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 2500, uuid(), 'Harry Potter Hogwarts', 'LEGO', {'pieces': '950', 'age': '10+', 'theme': 'Fantasy'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 120, uuid(), 'Mini Plane', 'Sluban', {'pieces': '50', 'age': '5+'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 4200, uuid(), 'Technic Liebherr Excavator', 'LEGO', {'pieces': '2000', 'age': '16+', 'motor': 'yes'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Конструктори', 950, uuid(), 'Friends Beach House', 'LEGO', {'pieces': '550', 'age': '7+', 'series': 'Friends'});
/* Ляльки */
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 600, uuid(), 'Barbie Fashionistas', 'Mattel', {'series': 'Fashion', 'material': 'plastic', 'height': '29cm'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 1200, uuid(), 'Barbie Dreamhouse Adventures', 'Mattel', {'set': 'doll+accessories', 'age': '3+'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 450, uuid(), 'Spider-Man Action Figure', 'Hasbro', {'hero': 'Marvel', 'articulation': 'yes', 'height': '15cm'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 2100, uuid(), 'Baby Born Interactive', 'Zapf Creation', {'functions': 'cry,eat,sleep', 'battery': 'no'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 350, uuid(), 'LOL Surprise Mini', 'MGA', {'type': 'blind_box', 'series': 'Glitter'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 900, uuid(), 'Elsa Frozen II', 'Hasbro', {'movie': 'Frozen', 'singing': 'yes'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 550, uuid(), 'Ken Doll', 'Mattel', {'series': 'Beach', 'gender': 'male'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 1800, uuid(), 'Rainbow High Fashion Doll', 'MGA', {'hair_color': 'rainbow', 'accessories': 'included'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 300, uuid(), 'Transformer Bumblebee', 'Hasbro', {'transform': 'car', 'difficulty': 'easy'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Ляльки', 850, uuid(), 'Monster High Draculaura', 'Mattel', {'theme': 'Gothic', 'year': '2023'});
/* НАСТІЛЬНІ ІГРИ */
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 1200, uuid(), 'Monopoly Classic', 'Hasbro', {'players': '2-6', 'time': '60-120min', 'lang': 'ua'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 250, uuid(), 'UNO Cards', 'Mattel', {'players': '2-10', 'type': 'card_game', 'age': '7+'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 1500, uuid(), 'Catan', 'Kosmos', {'players': '3-4', 'strategy': 'economic', 'time': '90min'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 800, uuid(), 'Dixit', 'Libellud', {'type': 'associative', 'art': 'abstract', 'players': '3-6'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 400, uuid(), 'Jenga', 'Hasbro', {'material': 'wood', 'type': 'dexterity'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 950, uuid(), 'Alias Party', 'Tactic', {'lang': 'ua', 'type': 'words', 'players': '4+'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 1800, uuid(), 'Scythe', 'Stonemaier Games', {'complexity': 'hard', 'theme': 'dieselpunk', 'time': '120min+'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 350, uuid(), 'Dobble', 'Asmodee', {'type': 'reaction', 'compact': 'yes'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 600, uuid(), 'Carcassonne', 'Hans im Gluck', {'type': 'tile_placement', 'theme': 'medieval'});
INSERT INTO items (category, price, id, name, producer, attributes) VALUES ('Настільні ігри', 2200, uuid(), 'Gloomhaven Jaws of the Lion', 'Cephalofair', {'type': 'rpg', 'campaign': 'yes', 'weight': 'heavy'});

/* ORDERS */
CREATE TABLE IF NOT EXISTS orders (
    customer_name text,
    order_time timestamp,
    order_id uuid,
    item_ids list<uuid>,
    total_price decimal,
    PRIMARY KEY ((customer_name), order_time, order_id)
) WITH CLUSTERING ORDER BY (order_time DESC);

INSERT INTO orders (customer_name, order_time, order_id, item_ids, total_price) VALUES ('Ivan', '2023-10-01 10:00:00', uuid(), [uuid(), uuid()], 2000);
INSERT INTO orders (customer_name, order_time, order_id, item_ids, total_price) VALUES ('Ivan', '2023-10-02 14:30:00', uuid(), [uuid()], 500);
INSERT INTO orders (customer_name, order_time, order_id, item_ids, total_price) VALUES ('Maria', '2023-10-01 12:00:00', uuid(), [uuid()], 150);
