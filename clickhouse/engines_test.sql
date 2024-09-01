/*standart merge tree engine*/
CREATE TABLE default.sales_merge_tree
(
    date Date,
    product String,
    quantity UInt32,
    price Float32,
    order_id UInt32
) ENGINE = MergeTree
ORDER BY date;

INSERT INTO default.sales_merge_tree (date, product, quantity, price, order_id) VALUES
('2023-05-01', 'Widget A', 10, 5.0, 1),
('2023-05-01', 'Widget A', 15, 4.5, 2),
('2023-05-02', 'Gadget B', 20, 10.0, 3);

SELECT * FROM sales_merge_tree;

/* analytics summing merge engine*/
CREATE TABLE default.sales_summing_merge_tree
(
    date Date,
    product String,
    quantity UInt32
) ENGINE = SummingMergeTree()
ORDER BY date;

INSERT INTO sales_summing_merge_tree (date, product, quantity) VALUES
('2023-05-01', 'Widget A', 10),
('2023-05-02', 'Gadget B', 20),
('2023-05-01', 'Widget A', 11),
('2023-05-02', 'Gadget B', 22),
('2023-05-01', 'Widget A', 12),
('2023-05-02', 'Gadget B', 23);

select * from sales_summing_merge_tree;

/* advanced aggregate engine*/
CREATE TABLE sales_raw 
(
    date_sale Date,
    product_id UInt32,
    quantity UInt32,
    price Float32
) ENGINE = MergeTree()
ORDER BY date_sale;

CREATE MATERIALIZED VIEW sales_mv
ENGINE = AggregatingMergeTree()
ORDER BY (date_sale, product_id) 
AS
SELECT
    date_sale,
    product_id,
    sumState(quantity) AS total_quantity,
    sumState(quantity * price) AS total_cost
FROM sales_raw
GROUP BY date_sale, product_id;

INSERT INTO sales_raw 
VALUES 
    ('2024-06-01', 1, 10, 100.0),
    ('2024-06-01', 1, 7, 100.0),
    ('2024-06-01', 2, 25, 200.0),
    ('2024-06-01', 2, 3, 200.0),
    ('2024-06-01', 2, 8, 200.0),
    ('2024-06-01', 1, 6, 100.0),
    ('2024-06-01', 2, 2, 200.0),
    ('2024-06-01', 2, 12, 200.0),
    ('2024-06-01', 2, 21, 200.0),
    ('2024-06-01', 1, 7, 100.0),
    ('2024-06-01', 2, 20, 200.0),
    ('2024-06-01', 2, 33, 200.0),
    ('2024-06-01', 2, 28, 200.0),
    ('2024-06-01', 1, 7, 100.0);

SELECT 
    date_sale, 
    product_id, 
    sumMerge(total_quantity) AS total_quantity, 
    sumMerge(total_cost) AS total_cost 
FROM 
    sales_mv 
GROUP BY 
    date_sale, product_id;
   
   
INSERT INTO sales_raw 
VALUES 
    ('2024-06-02', 1, 3, 100.0),
    ('2024-06-02', 1, 22, 100.0),
    ('2024-06-02', 2, 5, 200.0),
    ('2024-06-02', 2, 43, 200.0),
    ('2024-06-02', 2, 2, 200.0),
    ('2024-06-02', 1, 4, 100.0),
    ('2024-06-02', 2, 26, 200.0),
    ('2024-06-02', 2, 2, 200.0),
    ('2024-06-02', 2, 1, 200.0),
    ('2024-06-02', 1, 37, 100.0),
    ('2024-06-02', 2, 2, 200.0),
    ('2024-06-02', 2, 12, 200.0),
    ('2024-06-02', 2, 3, 200.0),
    ('2024-06-02', 1, 15, 100.0);
   
SELECT 
    date_sale, 
    product_id, 
    sumMerge(total_quantity) AS total_quantity, 
    sumMerge(total_cost) AS total_cost 
FROM 
    sales_mv 
GROUP BY 
    date_sale, product_id;

/* replacing engine */
CREATE TABLE default.replacing_MT
(
    key Int64,
    event_name String,
    eventTime DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO replacing_MT Values (1, 'test1', '2024-03-03 03:03:03');
INSERT INTO replacing_MT Values (1, 'test2', '2023-01-01 00:00:00');
INSERT INTO replacing_MT Values (1, 'test3', '2021-12-31 19:00:00');

SELECT * FROM replacing_MT final;

CREATE TABLE default.replacing_MT_with_param
(
    key Int64,
    event_name String,
    eventTime DateTime
)
ENGINE = ReplacingMergeTree (eventTime)
ORDER BY key;

INSERT INTO replacing_MT_with_param Values (1, 'test1', '2024-03-02 22:03:03');
INSERT INTO replacing_MT_with_param Values (1, 'test2', '2023-01-01 00:00:00');
INSERT INTO replacing_MT_with_param Values (1, 'test3', '2022-01-01 00:00:00');

SELECT * FROM replacing_MT_with_param final;

/* soft delete engine */
CREATE TABLE default.sales_collapsing_merge_tree
(
    date Date,
    product String,
    quantity UInt32,
    price Float32,
    order_id UInt32,
    sign Int8
) ENGINE = CollapsingMergeTree(sign)
ORDER BY date;

INSERT INTO sales_collapsing_merge_tree (date, product, quantity, price, order_id, sign) VALUES
('2023-05-01', 'Widget A', 10, 5.0, 1, 1),
('2023-05-01', 'Widget A', 10, 10.0, 1, 1),
('2023-05-02', 'Gadget B', 20, 10.0, 3, 1);

SELECT * FROM sales_collapsing_merge_tree;

SELECT * FROM sales_collapsing_merge_tree final;

/* versioning engine */
CREATE TABLE default.sales_versioned_collapsing_merge_tree
(
    date Date,
    product String,
    quantity UInt32,
    price Float32,
    order_id UInt32,
    sign Int8,
    version Int8
) ENGINE = VersionedCollapsingMergeTree(sign,version)
ORDER BY date;

INSERT INTO sales_versioned_collapsing_merge_tree (date, product, quantity, price, order_id, sign, version) VALUES
('2023-05-01', 'Widget A', 10, 5.0, 1, 1, 1);

INSERT INTO sales_versioned_collapsing_merge_tree (date, product, quantity, price, order_id, sign, version) VALUES
('2023-05-01', 'Widget A', 10, 5.0, 1, -1, 1),
('2023-05-01', 'Widget A', 15, 4.5, 2, 1, 2);

SELECT * FROM sales_versioned_collapsing_merge_tree final;