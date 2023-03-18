-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  --EXTRACT(MONTH from PARSE_DATE("%Y%m%d", date)) as month,
  count(fullVisitorId) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
group by month
order by month

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  distinct trafficSource.source,
  count(fullVisitorId) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  sum(totals.bounces)/count(fullVisitorId) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
SELECT 
  'week' as timetype,
  concat(2017,EXTRACT(week FROM PARSE_DATE("%Y%m%d", date))) as time,
  trafficSource.source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by timetype, time, trafficSource.source

union all
SELECT 
  'month' as timetype,
  concat(2017,EXTRACT(month FROM PARSE_DATE("%Y%m%d", date))) as time,
  trafficSource.source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by timetype, time, trafficSource.source

order by revenue DESC

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchase_table as (
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0601' and '0731' and totals.transactions >=1
group by month),

  non_purchase_table as (
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  sum(totals.pageviews)/count(distinct fullVisitorId) as 
avg_pageviews_non_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0601' and '0731' and totals.transactions is null
group by month)

SELECT purchase_table.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
from purchase_table
left join non_purchase_table
using(month)
order by month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  --sum(totals.transactions) as transactions,
  --count(distinct fullVisitorId) as visistorid,
  sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
where totals.transactions >= 1
group by month


-- Query 06: Average amount of money spent per session
#standardSQL
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  sum(totals.totalTransactionRevenue)/count(fullVisitorId) as 
avg_revenue_by_user_per_visit

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
where totals.transactions is not null
group by month


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

with B as (
SELECT 
  distinct fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) hits,
unnest(hits.product) product
WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
  and product.productRevenue is not null
  )

select 
    product.v2ProductName as other_purchased_products,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as A,
unnest(hits) hits,
unnest(hits.product) product
inner join B
on A.fullVisitorId = B.fullVisitorId
where product.productRevenue is not null
  and product.v2ProductName != "YouTube Men's Vintage Henley"
group by product.v2ProductName
order by quantity DESC

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  sum(case when eCommerceAction.action_type = "2" then 1 else null end) as num_product_view,
  sum(case when eCommerceAction.action_type = "3" then 1 else null end) as num_addtocart,
  sum(case when eCommerceAction.action_type = "6" then 1 else null end) as num_purchase,
  sum(case when eCommerceAction.action_type = "3" then 1 else null end)/sum(case when eCommerceAction.action_type = "2" then 1 else null end) as add_to_cart_rate,
   sum(case when eCommerceAction.action_type = "6" then 1 else null end)/sum(case when eCommerceAction.action_type = "2" then 1 else null end) as purchase_rate,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest(hits) hits,
unnest(product) product
where _table_suffix between '0101' and '0331'
group by month
order by month
