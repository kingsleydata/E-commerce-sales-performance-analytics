--THESE SERIES OF SQL QUERIES WERE TO USED TO ANALYZE ADVANCED E-COMMERCE METRICS 
--LIKE CUSTOMER REPEAT PURCHASE RATE, AVERAGE DUATION 
--BETWEEN FIRST AND SECOND PURCHASES BY CUSTOMERS AND MUCH MORE. 


--QUERY TO CALCULATE AVERAGE DURATION BETWEEN CUSTOMERS' FIRST AND SECOND PURCHASE IN DAYS
--THIS GIVES US A CLEAR UNDERSTANDING OF HOW QUICKLY OUR CUSTOMERS ARE LIKELY GOING TO COME
--BACK AFTER THEIR FIRST PURCHASE

--1.
	 WITH CUSTOMER_ORDER_COUNT AS 
        (
        SELECT customer_id, COUNT(INVOICE) AS NUMBER_OF_ORDERS
        FROM `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales`
        WHERE CUSTOMER_ID != 0
        GROUP BY 1
        HAVING NUMBER_OF_ORDERS > 1
        ),
        UNIQUE_CUSTOMER_ORDERS AS   
        (
        SELECT COC.CUSTOMER_ID, CAST(INVOICEDATE AS DATE) AS UNIQUE_ORDER_DATE, SUM(PRICE) AS TOTAL_ORDER
        FROM CUSTOMER_ORDER_COUNT COC
        LEFT JOIN `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales` ORS
        ON COC.CUSTOMER_ID = ORS.Customer_ID
        GROUP BY 1,2
        ),
        CUSTOMERS_WITH_1_OR_2_ORDERS AS    
        (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIQUE_ORDER_DATE) AS RN
        FROM UNIQUE_CUSTOMER_ORDERS
        QUALIFY RN <= 2
        ),
        CUSTOMER_FIRST_AND_SECOND_ORDERS AS   
        (
        SELECT CUSTOMER_ID, UNIQUE_ORDER_DATE,
        LEAD(UNIQUE_ORDER_DATE) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIQUE_ORDER_DATE) AS NEXT_ORDER_DATE, 
        DATE_DIFF(LEAD(UNIQUE_ORDER_DATE) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIQUE_ORDER_DATE),UNIQUE_ORDER_DATE, DAY)
        AS DATE_DIFFERENCE_BETWEEN_FIRST_AND_SECOND_CUSTOMER_ORDERS
        FROM CUSTOMERS_WITH_1_OR_2_ORDERS
        QUALIFY NEXT_ORDER_DATE IS NOT NULL
        )
        SELECT ROUND(AVG(DATE_DIFFERENCE_BETWEEN_FIRST_AND_SECOND_CUSTOMER_ORDERS), 2) 
        AS 
        AVERAGE_DURATION_BETWEEN_FIRST_AND_SECOND_ORDER_IN_DAYS
        FROM CUSTOMER_FIRST_AND_SECOND_ORDERS


--QUERY FOR NEW CUSTOMER ACQUISITION RATE
--THIS TELLS US HOW MANY NEW CUSTOMERS ARE WE ATTRACTING PER MONTH

--2.
            WITH UNIQUE_CUSTOMERS AS   
                (
                SELECT CUSTOMER_ID, CAST(INVOICEDATE AS DATE) AS ORDER_DATE,
                SUM(PRICE * QUANTITY) AS ORDER_TOTAL
                FROM `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales`
                WHERE CUSTOMER_ID  > 0
                AND (QUANTITY > 0
                AND PRICE > 0)
                GROUP BY 1,2
                ORDER BY 1,2
                ),
                FIRST_TIME_CUSTOMERS_AND_DATE_OF_PURCHASE AS   
                (
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CUSTOMER_ID) AS RN
                FROM UNIQUE_CUSTOMERS
                QUALIFY RN = 1
                ),
                CUSTOMER_ACQUISITION_BY_MONTH AS   
                (
                SELECT DATE_TRUNC(ORDER_DATE, MONTH) AS MONTH
                ,COUNT(CUSTOMER_ID) AS CUSTOMER_COUNT
                FROM FIRST_TIME_CUSTOMERS_AND_DATE_OF_PURCHASE
                GROUP BY MONTH
                --ORDER BY 1
                )
                SELECT MONTH,
                CUSTOMER_COUNT,
              AVG(CUSTOMER_COUNT) OVER (
    ORDER BY MONTH 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS ROLLING_3_MONTH_CUSTOMER_COUNT
                FROM CUSTOMER_ACQUISITION_BY_MONTH
                ORDER BY MONTH;
       

--QUERY FOR REPEAT PURCHASE RATE
--HOW EFFECTIVE ARE OUR PRODUCTS OR SERVICES, THIS TELLS US HOW MANY CUSTOMERS ARE COMING BACK AFTER THEIR FIRST
--PURCHASE OR AFTER MULTIPLE PURCHASES. A high repurchase rate indicates strong customer loyalty, satisfaction, and a positive
--perception of a business's products and services. It suggests that customers are not only satisfied with their initial purchase 
--but also willing to return and make repeat purchases, contributing to increased revenue and customer lifetime value.

--3.
   with customer_orders as 
                  (
                  SELECT CUSTOMER_ID, CAST(INVOICEDATE AS DATE) as customer_order_date, 
                  SUM(PRICE * Quantity) as order_amount
                  FROM `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales`
                  WHERE CUSTOMER_ID > 0
                  AND (QUANTITY > 0
                  AND PRICE > 0)
                  GROUP BY 1,2
                  --ORDER BY 1
                  ),
                  customers_with_repeat_purchases as   
                  (
                  select customer_id, customer_orders.customer_order_date,
                  row_number() over (partition by customer_id order by customer_order_date) AS RN
                  from customer_orders
                  QUALIFY RN > 1
                  ),
                  repeat_customers_table as   
                  (
                  select date_trunc(customer_order_date,month) as month, 
                  count (distinct customer_id) as repeat_customers
                  from customers_with_repeat_purchases
                  group by 1
                  ),
                  all_customers_table as   
                  (
                  select date_trunc(cast(invoicedate as date),month) as month,
                  count(distinct customer_id) as total_customers
                  from `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales`
                  WHERE CUSTOMER_ID > 0
                  AND (QUANTITY > 0
                  AND PRICE > 0)
                  group by 1
                  )
                  select rc.month as Month, repeat_customers, total_customers, 
                  round(repeat_customers/total_customers, 2) as repeat_purchase_rate
                  from repeat_customers_table rc
                  inner join all_customers_table ac
                  on rc.month = ac.month 
                  order by rc.month

--QUERY FOR REPEAT PURCHASE RATE BY PRODUCT
--REPEAT PRUCHASE RATE BY PRODUCT INDICATES THE NUMBER OF TIMES A PRODUCT WAS PURCHASED AGAIN
--AFTER THE FIRST PURCHASE BY THE SAME CUSTOMER
--IT INDICATES CUSTOMER SATISFACTION AND LOYALTY AS SUGGESTS CUSTOMERS ARE HAPPY WITH THE 
--PRODUCT OR SERVICE.
--IN THIS CASE I NOT ONLY CALCULATED THE  NUMBER OF UNQIUE CUSTOMERS THAT PURCHASED A PRODUCT
--AGAIN AFTER THEIR FIRST PURCHASE BUT I CALCULATED THE TOTAL NUMBER OF TIMES THEY PURCHASED 
--AFTER THE FIRST PURCHASE AND THIS CAN BE FILTERED BY COUNTRY AND MONTH IN THE DASHBOARD.

--4.
WITH REPEAT_CUSTOMER_ORDERS AS 
(
 SELECT DESCRIPTION, CUSTOMER_ID, INVOICEDATE, COUNTRY,
                ROW_NUMBER() OVER (PARTITION BY DESCRIPTION, CUSTOMER_ID ORDER BY DESCRIPTION, CUSTOMER_ID) AS RN
                FROM
                `campaign-analytics-project.ecommerce_ad_dataset.online_retail_sales`
                 WHERE 1=1
                AND CUSTOMER_ID > 0
                AND (QUANTITY > 0
                AND PRICE > 0)
                QUALIFY RN > 1
)
SELECT DESCRIPTION AS PRODUCT_NAME, COUNT(CUSTOMER_ID) AS NUMBER_OF_REPEAT_PURCHASES
FROM REPEAT_CUSTOMER_ORDERS
GROUP BY 1
ORDER BY 2 DESC



