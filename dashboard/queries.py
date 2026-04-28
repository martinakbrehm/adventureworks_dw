"""
Consultas SQL para o dashboard AdventureWorks DW.
Todas as queries operam na camada mart do Snowflake (schema DBT_TRANSFORMED).
"""

# ── KPIs Gerais ────────────────────────────────────────────────────────────────
KPI_GENERAL = """
SELECT
    COUNT(DISTINCT salesorderid)                        AS total_orders,
    COUNT(DISTINCT customerid)                          AS total_customers,
    ROUND(SUM(totaldue), 2)                             AS total_revenue,
    ROUND(AVG(totaldue), 2)                             AS avg_order_value,
    ROUND(SUM(amountpaidproduct), 2)                    AS total_product_revenue,
    ROUND(SUM(orderqty), 0)                             AS total_items_sold
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado');
"""

# ── Receita Mensal ─────────────────────────────────────────────────────────────
MONTHLY_REVENUE = """
SELECT
    DATE_TRUNC('month', orderdate)                      AS order_month,
    ROUND(SUM(totaldue), 2)                             AS monthly_revenue,
    COUNT(DISTINCT salesorderid)                        AS monthly_orders
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado')
GROUP BY 1
ORDER BY 1;
"""

# ── Top 10 Produtos por Receita ────────────────────────────────────────────────
TOP_PRODUCTS = """
SELECT
    dp.product_name,
    dp.productsubcategory_name                          AS subcategory,
    dp.productcategory_name                             AS category,
    ROUND(SUM(fs.amountpaidproduct), 2)                 AS total_revenue,
    SUM(fs.orderqty)                                    AS total_qty
FROM fact_sales fs
JOIN dim_products dp ON fs.productfk = dp.productsk
WHERE fs.statussales NOT IN ('Cancelado', 'Rejeitado')
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 10;
"""

# ── Receita por País ───────────────────────────────────────────────────────────
REVENUE_BY_COUNTRY = """
SELECT
    countryregion_name                                  AS country,
    ROUND(SUM(totaldue), 2)                             AS total_revenue,
    COUNT(DISTINCT salesorderid)                        AS total_orders
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado')
  AND countryregion_name IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
"""

# ── Canal de Venda: Online vs Offline ─────────────────────────────────────────
SALES_CHANNEL = """
SELECT
    CASE WHEN onlineorderflag = TRUE THEN 'Online' ELSE 'Loja Física' END AS channel,
    COUNT(DISTINCT salesorderid)                        AS total_orders,
    ROUND(SUM(totaldue), 2)                             AS total_revenue
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado')
GROUP BY 1;
"""

# ── Distribuição de Motivos de Venda ──────────────────────────────────────────
SALES_REASONS = """
SELECT
    SUM(price)          AS price_count,
    SUM(manufacturer)   AS manufacturer_count,
    SUM(quality)        AS quality_count,
    SUM(promotion)      AS promotion_count,
    SUM(review)         AS review_count,
    SUM(other)          AS other_count,
    SUM(television)     AS television_count
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado');
"""

# ── Status dos Pedidos ─────────────────────────────────────────────────────────
ORDER_STATUS = """
SELECT
    statussales                                         AS status,
    COUNT(DISTINCT salesorderid)                        AS total_orders,
    ROUND(SUM(totaldue), 2)                             AS total_revenue
FROM fact_sales
GROUP BY 1
ORDER BY 2 DESC;
"""

# ── Força de Vendas por Região e Gênero ───────────────────────────────────────
SELLERS_BY_REGION = """
SELECT
    countryregion_name                                  AS country,
    gender,
    jobtitle,
    SUM(totalsalesorders)                               AS total_orders
FROM fact_aggsalessellersregion
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
"""

# ── Receita por Categoria de Produto ─────────────────────────────────────────
REVENUE_BY_CATEGORY = """
SELECT
    dp.productcategory_name                             AS category,
    ROUND(SUM(fs.amountpaidproduct), 2)                 AS total_revenue,
    ROUND(SUM(fs.orderqty * fs.standardcost), 2)        AS total_cost,
    ROUND(SUM(fs.amountpaidproduct) - SUM(fs.orderqty * fs.standardcost), 2) AS gross_profit
FROM fact_sales fs
JOIN dim_products dp ON fs.productfk = dp.productsk
WHERE fs.statussales NOT IN ('Cancelado', 'Rejeitado')
  AND dp.productcategory_name IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
"""

# ── Receita Trimestral ─────────────────────────────────────────────────────────
QUARTERLY_REVENUE = """
SELECT
    YEAR(orderdate)                                     AS year,
    QUARTER(orderdate)                                  AS quarter,
    CONCAT('Q', QUARTER(orderdate), ' ', YEAR(orderdate)) AS period,
    ROUND(SUM(totaldue), 2)                             AS quarterly_revenue
FROM fact_sales
WHERE statussales NOT IN ('Cancelado', 'Rejeitado')
GROUP BY 1, 2, 3
ORDER BY 1, 2;
"""
