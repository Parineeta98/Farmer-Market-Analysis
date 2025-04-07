-- Vendor's table
Create Table vendors (
vendor_id INT PRIMARY KEY,
vendor_name varchar(100),
category varchar(50)
);

--Weather table
Create Table weather (
date DATE PRIMARY KEY,
temperature INT,
rainfall_mm FLOAT
);

-- Sales table
Create Table sales(
sale_id INT PRIMARY KEY,
vendor_id INT,
date DATE,
product varchar(100),
quantity INT,
unit_price FLOAT,
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
    FOREIGN KEY (date) REFERENCES weather(date)
);

BULK INSERT vendors
FROM 'D:\SQL\Datasets\farmermarketsales\vendors.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    CODEPAGE = 'ACP'
);

BULK INSERT sales
FROM 'D:\SQL\Datasets\farmermarketsales\sales.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = 'ACP'
);

BULK INSERT weather
FROM 'D:\SQL\Datasets\farmermarketsales\weather.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = 'ACP'
);

-- Calculating total revenue per vendor 
WITH SalesRevenue AS (
    SELECT 
        s.sale_id,
        s.vendor_id,
        s.date,
        (s.quantity * s.unit_price) AS revenue
    FROM sales s
)

SELECT 
    v.vendor_name,
    v.category,
    ROUND(SUM(sr.revenue), 2) AS total_revenue
FROM SalesRevenue sr
JOIN vendors v
    ON sr.vendor_id = v.vendor_id
GROUP BY v.vendor_name, v.category
ORDER BY total_revenue DESC;

-- Monthly Sales
-- Creating a temporary table to hold monthly sales totals
IF OBJECT_ID('tempdb..#MonthlySales') IS NOT NULL
    DROP TABLE #MonthlySales;

SELECT 
    CONVERT(VARCHAR(7), s.date, 120) AS sale_month, -- YYYY-MM format
    SUM(s.quantity * s.unit_price) AS monthly_revenue
INTO #MonthlySales
FROM sales s
GROUP BY CONVERT(VARCHAR(7), s.date, 120);

SELECT sale_month, ROUND(monthly_revenue, 2) AS monthly_revenue
FROM #MonthlySales
ORDER BY sale_month;

--Checking impact of weather on sales
-- Daily revenue with weather condition classification
WITH DailySales AS (
    SELECT 
        s.date,
        SUM(s.quantity * s.unit_price) AS daily_revenue
    FROM sales s
    GROUP BY s.date
),
WeatherClassification AS (
    SELECT 
        w.date,
        w.temperature,
        w.rainfall_mm,
        CASE 
            WHEN w.rainfall_mm > 0 THEN 'Rainy'
            ELSE 'Not Rainy'
        END AS weather_type
    FROM weather w
)
SELECT 
    d.date,
    d.daily_revenue,
    w.temperature,
    w.rainfall_mm,
    w.weather_type
FROM DailySales d
JOIN WeatherClassification w
    ON d.date = w.date

UNION ALL

-- Summary row: Total revenue by weather type over the entire period
SELECT 
    NULL AS date,
    ROUND(SUM(daily_revenue), 2) AS daily_revenue,
    NULL AS temperature,
    NULL AS rainfall_mm,
    weather_type
FROM (
    SELECT 
        d.date,
        d.daily_revenue,
        CASE 
            WHEN w.rainfall_mm > 0 THEN 'Rainy'
            ELSE 'Not Rainy'
        END AS weather_type
    FROM DailySales d
    JOIN weather w ON d.date = w.date
) AS Summary
GROUP BY weather_type;

-- Identifying top 5 prodcuts by revenue
WITH ProductRevenue AS (
    SELECT 
        s.product,
        SUM(s.quantity * s.unit_price) AS revenue,
        CASE 
            WHEN s.product LIKE '%Honey%' THEN 'Honey Products'
            WHEN s.product LIKE '%Loaf%' OR s.product LIKE '%Bread%' THEN 'Baked Goods'
            WHEN s.product LIKE '%Juice%' OR s.product LIKE '%Smoothie%' THEN 'Beverages'
            ELSE 'Other'
        END AS product_category
    FROM sales s
    GROUP BY s.product
)
SELECT TOP 5
    product,
    product_category,
    ROUND(revenue, 2) AS revenue
FROM ProductRevenue
ORDER BY revenue DESC;

-- Revenue vs Weather analysis
WITH WeatherImpact AS (
    SELECT 
        s.date,
        SUM(s.quantity * s.unit_price) AS revenue,
        w.rainfall_mm,
        w.temperature,
        CASE 
            WHEN w.rainfall_mm > 0 THEN 'Rainy'
            ELSE 'Clear'
        END AS weather_type
    FROM sales s
    JOIN weather w ON s.date = w.date
    GROUP BY s.date, w.rainfall_mm, w.temperature
)
SELECT weather_type, 
       ROUND(AVG(revenue), 2) AS avg_revenue,
       ROUND(AVG(temperature), 1) AS avg_temp
FROM WeatherImpact
GROUP BY weather_type;

--Vendor performance ranking
WITH VendorRevenue AS (
    SELECT 
        vendor_id,
        SUM(quantity * unit_price) AS total_revenue
    FROM sales
    GROUP BY vendor_id
)
SELECT 
    v.vendor_name,
    vr.total_revenue,
    RANK() OVER (ORDER BY vr.total_revenue DESC) AS revenue_rank
FROM VendorRevenue vr
JOIN vendors v ON vr.vendor_id = v.vendor_id;

--Basket Analysis
SELECT 
    s1.product AS product_a,
    s2.product AS product_b,
    COUNT(*) AS times_bought_together
FROM sales s1
JOIN sales s2 
    ON s1.date = s2.date 
    AND s1.vendor_id = s2.vendor_id 
    AND s1.sale_id <> s2.sale_id
WHERE s1.product < s2.product  -- avoid duplicates
GROUP BY s1.product, s2.product
HAVING COUNT(*) > 3
ORDER BY times_bought_together DESC;

--Product Category monthly performace 
WITH CategorizedSales AS (
    SELECT 
        s.date,
        CASE 
            WHEN s.product LIKE '%Juice%' THEN 'Beverages'
            WHEN s.product LIKE '%Loaf%' THEN 'Bakery'
            WHEN s.product LIKE '%Honey%' THEN 'Honey'
            ELSE 'Other'
        END AS category,
        (s.quantity * s.unit_price) AS revenue
    FROM sales s
)
SELECT 
    FORMAT(date, 'yyyy-MM') AS sale_month,
    category,
    SUM(revenue) AS total_revenue
FROM CategorizedSales
GROUP BY FORMAT(date, 'yyyy-MM'), category
ORDER BY sale_month;

-- New vs returning vendor sales
WITH FirstSale AS (
    SELECT 
        vendor_id,
        MIN(date) AS first_sale_date
    FROM sales
    GROUP BY vendor_id
)
SELECT 
    s.vendor_id,
    v.vendor_name,
    s.date,
    CASE 
        WHEN s.date = fs.first_sale_date THEN 'New'
        ELSE 'Returning'
    END AS vendor_type,
    SUM(s.quantity * s.unit_price) AS revenue
FROM sales s
JOIN FirstSale fs ON s.vendor_id = fs.vendor_id
JOIN vendors v ON s.vendor_id = v.vendor_id
GROUP BY s.vendor_id, v.vendor_name, s.date, fs.first_sale_date;


