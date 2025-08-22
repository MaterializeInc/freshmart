-- Heartbeat and refresh tracking tables
CREATE TABLE heartbeats (
	id SERIAL PRIMARY KEY,
	ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE materialized_view_refresh_log (
	view_name TEXT PRIMARY KEY,
	last_refresh TIMESTAMP DEFAULT now(),
	refresh_duration DOUBLE PRECISION DEFAULT 0
);

-- Base tables
CREATE TABLE products (
	product_id SERIAL PRIMARY KEY,
	product_name VARCHAR(255) NOT NULL,
	base_price NUMERIC(10, 2) NOT NULL,
	category_id INTEGER NOT NULL,
	supplier_id INTEGER NOT NULL,
	available BOOLEAN NOT NULL,
	last_update_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE categories (
	category_id SERIAL PRIMARY KEY,
	category_name VARCHAR(255) NOT NULL,
	parent_id INT
);

CREATE TABLE suppliers (
	supplier_id SERIAL PRIMARY KEY,
	supplier_name VARCHAR(255) NOT NULL
);

CREATE TABLE sales (
	sale_id SERIAL PRIMARY KEY,
	product_id INTEGER NOT NULL,
	sale_price NUMERIC(10, 2) NOT NULL,
	sale_date TIMESTAMP NOT NULL,
	price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE inventory (
	inventory_id SERIAL PRIMARY KEY,
	product_id INTEGER NOT NULL,
	stock INTEGER NOT NULL,
	warehouse_id INTEGER NOT NULL,
	restock_date TIMESTAMP NOT NULL
);

CREATE TABLE promotions (
	promotion_id SERIAL PRIMARY KEY,
	product_id INTEGER NOT NULL,
	promotion_discount NUMERIC(10, 2) NOT NULL,
	start_date TIMESTAMP NOT NULL,
	end_date TIMESTAMP NOT NULL,
	active BOOLEAN NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE shopping_cart (
    product_id INT NOT NULL,
	product_name TEXT NOT NULL,
	category_id INT NOT NULL,
	price NUMERIC(10, 2) NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE public.inventory ADD CONSTRAINT inventory_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products (product_id);
ALTER TABLE public.promotions ADD CONSTRAINT promotions_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products (product_id);
ALTER TABLE public.sales ADD CONSTRAINT sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products (product_id);

CREATE INDEX idx_products_product_name ON products(product_name);
CREATE INDEX idx_sales_product_id ON sales(product_id);
CREATE INDEX idx_sales_sale_date ON sales(sale_date);
CREATE INDEX idx_sales_product_id_sale_date ON sales(product_id, sale_date);
CREATE INDEX idx_promotions_product_id ON promotions(product_id);
CREATE INDEX idx_promotions_active ON promotions(active);
CREATE INDEX idx_promotions_product_id_active ON promotions(product_id, active);
CREATE INDEX idx_inventory_product_id ON inventory(product_id);

CREATE VIEW inventory_item AS
WITH recent_prices AS (
	SELECT grp.product_id, AVG(price) AS avg_price
	FROM (SELECT DISTINCT product_id FROM sales) grp, 
	LATERAL (
		SELECT product_id, price 
		FROM sales
		WHERE sales.product_id = grp.product_id 
		ORDER BY sale_date DESC LIMIT 10
	) sub
	GROUP BY grp.product_id
),

promotion_effect AS (
	SELECT 
		p.product_id,
		MIN(pr.promotion_discount) AS promotion_discount
	FROM promotions pr
	JOIN products p ON pr.product_id = p.product_id
	WHERE pr.active = TRUE
	GROUP BY p.product_id
),

popularity_score AS (
	SELECT 
		s.product_id,
		RANK() OVER (PARTITION BY p.category_id ORDER BY COUNT(s.sale_id) DESC) AS popularity_rank,
		COUNT(s.sale_id) AS sale_count
	FROM sales s
	JOIN products p ON s.product_id = p.product_id
	GROUP BY s.product_id, p.category_id
),

inventory_status AS (
	SELECT 
		i.product_id,
		SUM(i.stock) AS total_stock,
		RANK() OVER (ORDER BY SUM(i.stock) DESC) AS stock_rank
	FROM inventory i
	GROUP BY i.product_id
),

high_demand_products AS (
	SELECT 
		p.product_id,
		AVG(s.sale_price) AS avg_sale_price,
		COUNT(s.sale_id) AS total_sales
	FROM products p
	JOIN sales s ON p.product_id = s.product_id
	GROUP BY p.product_id
	HAVING COUNT(s.sale_id) > (SELECT AVG(total_sales) FROM (SELECT COUNT(*) AS total_sales FROM sales GROUP BY product_id) subquery)
),

item_enriched AS (
	SELECT 
		p.product_id,
		p.base_price,
		CASE 
			WHEN pop.popularity_rank <= 3 THEN 1.2
			WHEN pop.popularity_rank BETWEEN 4 AND 10 THEN 1.1
			ELSE 0.9
		END AS popularity_adjustment,
		rp.avg_price,
		COALESCE(1.0 - (pe.promotion_discount / 100), 1) AS promotion_discount,
		CASE 
			WHEN inv.stock_rank <= 3 THEN 1.1
			WHEN inv.stock_rank BETWEEN 4 AND 10 THEN 1.05
			ELSE 1
		END AS stock_adjustment,
		CASE 
			WHEN p.base_price > rp.avg_price THEN 1 + (p.base_price - rp.avg_price) / rp.avg_price
			ELSE 1 - (rp.avg_price - p.base_price) / rp.avg_price
		END AS demand_multiplier,
		hd.avg_sale_price,
		CASE 
			WHEN p.product_name ilike '%cheap%' THEN 0.8
			ELSE 1.0
		END AS additional_discount
	FROM products p 
	LEFT JOIN recent_prices rp ON p.product_id = rp.product_id
	LEFT JOIN promotion_effect pe ON p.product_id = pe.product_id
	JOIN popularity_score pop ON p.product_id = pop.product_id
	LEFT JOIN inventory_status inv ON p.product_id = inv.product_id
	LEFT JOIN high_demand_products hd ON p.product_id = hd.product_id
)
SELECT 
	ie.product_id,
	ie.base_price * ie.popularity_adjustment * ie.promotion_discount * ie.stock_adjustment * ie.demand_multiplier * ie.additional_discount AS live_price,
	p.last_update_time
FROM item_enriched ie
JOIN products p ON ie.product_id = p.product_id;

-- Contextual comments
COMMENT ON VIEW inventory_item IS 
'This view is the canonical representation of an inventory item. 
It provides a unified way to reference a product in the system, 
with its core identity (product_id), its current operational price (live_price), 
and the last time the underlying product record was updated (last_update_time). 
Think of this as the "single source of truth" for inventory items within the business context.';

COMMENT ON COLUMN inventory_item.product_id IS 
'The unique identifier for the inventory item. 
This connects directly to the products table and allows other systems or queries to join consistently on a stable ID.';

COMMENT ON COLUMN inventory_item.live_price IS 
'The current selling price of the inventory item. 
It is not a fixed value in the products table but a property that reflects present business conditions. 
When another system or dashboard asks, "What is the price of this item right now?" — this is the field to use.';

COMMENT ON COLUMN inventory_item.last_update_time IS 
'The timestamp of the most recent update to the product record. 
This tells you how fresh the information is. 
If you are checking for stale data or debugging why a price looks off, this column is a good reference point.';


CREATE MATERIALIZED VIEW mv_inventory_item AS
WITH recent_prices AS (
	SELECT grp.product_id, AVG(price) AS avg_price
	FROM (SELECT DISTINCT product_id FROM sales) grp,
	LATERAL (
		SELECT product_id, price
		FROM sales
		WHERE sales.product_id = grp.product_id
		ORDER BY sale_date DESC LIMIT 10
	) sub
	GROUP BY grp.product_id
),
promotion_effect AS (
	SELECT
		p.product_id,
		MIN(pr.promotion_discount) AS promotion_discount
	FROM promotions pr
	JOIN products p ON pr.product_id = p.product_id
	WHERE pr.active = TRUE
	GROUP BY p.product_id
),
popularity_score AS (
	SELECT
		s.product_id,
		RANK() OVER (PARTITION BY p.category_id ORDER BY COUNT(s.sale_id) DESC) AS popularity_rank,
		COUNT(s.sale_id) AS sale_count
	FROM sales s
	JOIN products p ON s.product_id = p.product_id
	GROUP BY s.product_id, p.category_id
),
inventory_status AS (
	SELECT
		i.product_id,
		SUM(i.stock) AS total_stock,
		RANK() OVER (ORDER BY SUM(i.stock) DESC) AS stock_rank
	FROM inventory i
	GROUP BY i.product_id
),
high_demand_products AS (
	SELECT
		p.product_id,
		AVG(s.sale_price) AS avg_sale_price,
		COUNT(s.sale_id) AS total_sales
	FROM products p
	JOIN sales s ON p.product_id = s.product_id
	GROUP BY p.product_id
	HAVING COUNT(s.sale_id) > (SELECT AVG(total_sales) FROM (SELECT COUNT(*) AS total_sales FROM sales GROUP BY product_id) subquery)
),
item_enriched AS (
	SELECT
		p.product_id,
		p.base_price,
		CASE
			WHEN pop.popularity_rank <= 3 THEN 1.2
			WHEN pop.popularity_rank BETWEEN 4 AND 10 THEN 1.1
			ELSE 0.9
		END AS popularity_adjustment,
		rp.avg_price,
		COALESCE(1.0 - (pe.promotion_discount / 100), 1) AS promotion_discount,
		CASE
			WHEN inv.stock_rank <= 3 THEN 1.1
			WHEN inv.stock_rank BETWEEN 4 AND 10 THEN 1.05
			ELSE 1
		END AS stock_adjustment,
		CASE
			WHEN p.base_price > rp.avg_price THEN 1 + (p.base_price - rp.avg_price) / rp.avg_price
			ELSE 1 - (rp.avg_price - p.base_price) / rp.avg_price
		END AS demand_multiplier,
		hd.avg_sale_price,
		CASE
			WHEN p.product_name ilike '%cheap%' THEN 0.8
			ELSE 1.0
		END AS additional_discount
	FROM products p
	LEFT JOIN recent_prices rp ON p.product_id = rp.product_id
	LEFT JOIN promotion_effect pe ON p.product_id = pe.product_id
	JOIN popularity_score pop ON p.product_id = pop.product_id
	LEFT JOIN inventory_status inv ON p.product_id = inv.product_id
	LEFT JOIN high_demand_products hd ON p.product_id = hd.product_id
)
SELECT
	ie.product_id,
	ie.base_price * ie.popularity_adjustment * ie.promotion_discount * ie.stock_adjustment * ie.demand_multiplier * ie.additional_discount AS live_price,
	p.last_update_time
FROM item_enriched ie
JOIN products p ON ie.product_id = p.product_id;

CREATE INDEX idx_mv_inventory_item_product_id ON mv_inventory_item(product_id);

-- Initialize the refresh log
INSERT INTO materialized_view_refresh_log (view_name, last_refresh)
VALUES ('mv_inventory_item', now())
ON CONFLICT (view_name)
DO UPDATE SET last_refresh = EXCLUDED.last_refresh;


ALTER TABLE categories REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
ALTER TABLE materialized_view_refresh_log REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE promotions REPLICA IDENTITY FULL;
ALTER TABLE sales REPLICA IDENTITY FULL;
ALTER TABLE suppliers REPLICA IDENTITY FULL;
ALTER TABLE shopping_cart REPLICA IDENTITY FULL;
ALTER TABLE heartbeats REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR TABLE categories, inventory, materialized_view_refresh_log, products, promotions, sales, suppliers, heartbeats, shopping_cart;