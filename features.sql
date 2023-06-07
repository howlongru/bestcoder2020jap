WITH feature_engineering AS (

  SELECT 
  userid,
  DATE_DIFF(MAX(grass_date), CAST('2020-08-01' AS DATE), DAY) AS max_date, # 202002-07
  DATE_DIFF(MIN(grass_date), CAST('2020-08-01' AS DATE), DAY) AS min_date,
  # order_count
  SUM(IF(grass_date >= '2020-07-01', order_count, 0)) AS sum_order_count_L1M,
  SUM(IF(grass_date >= '2020-06-01', order_count, 0)) AS sum_order_count_L2M,
  SUM(IF(grass_date >= '2020-05-01', order_count, 0)) AS sum_order_count_L3M,
  SUM(IF(grass_date >= '2020-04-01', order_count, 0)) AS sum_order_count_L4M, 
  SUM(IF(grass_date >= '2020-03-01', order_count, 0)) AS sum_order_count_L5M,
  SUM(IF(grass_date >= '2020-02-01', order_count, 0)) AS sum_order_count_ALL, 
  # total_amount
  SUM(IF(grass_date >= '2020-07-01', total_amount, 0)) AS sum_total_amount_L1M,
  SUM(IF(grass_date >= '2020-06-01', total_amount, 0)) AS sum_total_amount_L2M,
  SUM(IF(grass_date >= '2020-05-01', total_amount, 0)) AS sum_total_amount_L3M,
  SUM(IF(grass_date >= '2020-04-01', total_amount, 0)) AS sum_total_amount_L4M, 
  SUM(IF(grass_date >= '2020-03-01', total_amount, 0)) AS sum_total_amount_L5M,
  SUM(IF(grass_date >= '2020-02-01', total_amount, 0)) AS sum_total_amount_ALL, 
  # category_encoded
  SUM(IF(grass_date >= '2020-07-01', category_encoded, 0)) AS sum_category_encoded_L1M,
  SUM(IF(grass_date >= '2020-06-01', category_encoded, 0)) AS sum_category_encoded_L2M,
  SUM(IF(grass_date >= '2020-05-01', category_encoded, 0)) AS sum_category_encoded_L3M,
  SUM(IF(grass_date >= '2020-04-01', category_encoded, 0)) AS sum_category_encoded_L4M, 
  SUM(IF(grass_date >= '2020-03-01', category_encoded, 0)) AS sum_category_encoded_L5M,
  SUM(IF(grass_date >= '2020-02-01', category_encoded, 0)) AS sum_category_encoded_ALL
  FROM bq_project.SP.purchase_detail
  GROUP BY userid
  
), monthly_averge AS (

  SELECT
  userid,
  # order_count - monthly average
  AVG(IF(grass_month >= '2020-07-01', monthly_order_count, 0)) AS avg_monthly_order_count_L1M,
  AVG(IF(grass_month >= '2020-06-01', monthly_order_count, 0)) AS avg_monthly_order_count_L2M,
  AVG(IF(grass_month >= '2020-05-01', monthly_order_count, 0)) AS avg_monthly_order_count_L3M,
  AVG(IF(grass_month >= '2020-04-01', monthly_order_count, 0)) AS avg_monthly_order_count_L4M, 
  AVG(IF(grass_month >= '2020-03-01', monthly_order_count, 0)) AS avg_monthly_order_count_L5M,
  AVG(IF(grass_month >= '2020-02-01', monthly_order_count, 0)) AS avg_monthly_order_count_ALL,
  # total_amount - monthly average
  AVG(IF(grass_month >= '2020-07-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_L1M,
  AVG(IF(grass_month >= '2020-06-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_L2M,
  AVG(IF(grass_month >= '2020-05-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_L3M,
  AVG(IF(grass_month >= '2020-04-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_L4M, 
  AVG(IF(grass_month >= '2020-03-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_L5M,
  AVG(IF(grass_month >= '2020-02-01', monthly_total_amount, 0)) AS avg_monthly_total_amount_ALL,
  # category_encoded - monthly average
  AVG(IF(grass_month >= '2020-07-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_L1M,
  AVG(IF(grass_month >= '2020-06-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_L2M,
  AVG(IF(grass_month >= '2020-05-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_L3M,
  AVG(IF(grass_month >= '2020-04-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_L4M, 
  AVG(IF(grass_month >= '2020-03-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_L5M,
  AVG(IF(grass_month >= '2020-02-01', monthly_category_encoded, 0)) AS avg_monthly_category_encoded_ALL 
  FROM (
    SELECT
    userid,
    DATE_TRUNC(grass_date, MONTH) AS grass_month,
    SUM(order_count) AS monthly_order_count,
    SUM(total_amount) AS monthly_total_amount,
    SUM(category_encoded) AS monthly_category_encoded
    FROM bq_project.SP.purchase_detail
    GROUP BY userid, grass_month
  )
  GROUP BY userid

), category_agg
AS (

  SELECT
  userid,
  SUM(IF(category_encoded = 1, total_amount, 0))  AS category_encoded_total_amount_1,
  SUM(IF(category_encoded = 2, total_amount, 0))  AS category_encoded_total_amount_2,
  SUM(IF(category_encoded = 3, total_amount, 0))  AS category_encoded_total_amount_3,
  SUM(IF(category_encoded = 4, total_amount, 0))  AS category_encoded_total_amount_4,
  SUM(IF(category_encoded = 5, total_amount, 0))  AS category_encoded_total_amount_5,
  SUM(IF(category_encoded = 6, total_amount, 0))  AS category_encoded_total_amount_6,
  SUM(IF(category_encoded = 7, total_amount, 0))  AS category_encoded_total_amount_7,
  SUM(IF(category_encoded = 8, total_amount, 0))  AS category_encoded_total_amount_8,
  SUM(IF(category_encoded = 9, total_amount, 0))  AS category_encoded_total_amount_9,
  SUM(IF(category_encoded = 10, total_amount, 0)) AS category_encoded_total_amount_10,
  SUM(IF(category_encoded = 11, total_amount, 0)) AS category_encoded_total_amount_11,
  SUM(IF(category_encoded = 12, total_amount, 0)) AS category_encoded_total_amount_12,
  SUM(IF(category_encoded = 13, total_amount, 0))  AS category_encoded_total_amount_13,
  SUM(IF(category_encoded = 14, total_amount, 0))  AS category_encoded_total_amount_14,
  SUM(IF(category_encoded = 15, total_amount, 0))  AS category_encoded_total_amount_15,
  SUM(IF(category_encoded = 16, total_amount, 0))  AS category_encoded_total_amount_16,
  SUM(IF(category_encoded = 17, total_amount, 0))  AS category_encoded_total_amount_17,
  SUM(IF(category_encoded = 18, total_amount, 0))  AS category_encoded_total_amount_18,
  SUM(IF(category_encoded = 19, total_amount, 0))  AS category_encoded_total_amount_19,
  SUM(IF(category_encoded = 20, total_amount, 0)) AS category_encoded_total_amount_20,
  SUM(IF(category_encoded = 21, total_amount, 0)) AS category_encoded_total_amount_21,
  SUM(IF(category_encoded = 22, total_amount, 0)) AS category_encoded_total_amount_22,
  SUM(IF(category_encoded = 23, total_amount, 0)) AS category_encoded_total_amount_23,
  FROM bq_project.SP.purchase_detail
  GROUP BY userid
), login AS (
    SELECT userid, date, login_times
    FROM `bq_project.SP.login`
), purchase AS (
    SELECT userid, grass_date, order_count, total_amount, category_encoded
    FROM `bq_project.SP.purchase_detail`
), user_info AS (
    SELECT userid, gender, is_seller, birth_year, enroll_time
    FROM `bq_project.SP.user_info`
), train AS (
    SELECT userid, label
    FROM `bq_project.SP.train`
), login_sum AS (
    SELECT 
        userid, 
        SUM(login_times) AS login_total_times,
        COUNT(DISTINCT date) AS login_total_days,
        COUNT(DISTINCT DATE_TRUNC(date, MONTH)) AS total_months,
        DATE_DIFF('2020-07-31', MAX(date), DAY) AS last_date_from_0731,
        DATE_DIFF(MIN(date), '2020-02-01', DAY) AS first_date_from_0201,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-02-01', login_times, 0)) AS login_m2_times,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-03-01', login_times, 0)) AS login_m3_times,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-04-01', login_times, 0)) AS login_m4_times,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-05-01', login_times, 0)) AS login_m5_times,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-06-01', login_times, 0)) AS login_m6_times,
        SUM(IF(DATE_TRUNC(date, MONTH) = '2020-07-01', login_times, 0)) AS login_m7_times,
        SUM(IF(DATE_TRUNC(date, MONTH) >= '2020-03-01', login_times, 0)) AS login_af_m3_times,
        SUM(IF(DATE_TRUNC(date, MONTH) >= '2020-04-01', login_times, 0)) AS login_af_m4_times,
        SUM(IF(DATE_TRUNC(date, MONTH) >= '2020-05-01', login_times, 0)) AS login_af_m5_times,
        SUM(IF(DATE_TRUNC(date, MONTH) >= '2020-06-01', login_times, 0)) AS login_af_m6_times
    FROM login
    GROUP BY userid
), user_info_sum AS (
    SELECT 
        userid,
        CAST(gender AS STRING) AS info_gender,
        is_seller AS info_is_seller,
        birth_year AS info_birth_year,
        ROUND(2021- birth_year) AS info_age,
        DATE_DIFF('2020-07-31', enroll_time, DAY) AS info_enroll_day
    FROM user_info
)

-- ), main_user_table AS (

--   SELECT userid, label FROM bq_project.SP.train
--   UNION ALL
--   SELECT userid, NULL AS label FROM bq_project.SP.submission 
  
-- ), union_all_table AS (

  SELECT
  a.*, 
  b.*EXCEPT(userid),
  c.*EXCEPT(userid),
  d.*EXCEPT(userid),
  e.*EXCEPT(userid),
  f.*EXCEPT(userid)
--   FROM `bq_project.SP.train` a
  FROM  `bq_project.SP.submission` a
  LEFT JOIN feature_engineering b 
  USING(userid)
  LEFT JOIN monthly_averge c 
  USING(userid)
  LEFT JOIN login_sum d
  USING(userid)
  LEFT JOIN user_info_sum e
  USING(userid)
  LEFT JOIN category_agg f
  USING(userid)
  

