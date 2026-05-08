CREATE TABLE IF NOT EXISTS d_customer (
  customer_id   STRING,
  full_name     STRING,
  email         STRING,
  country       STRING,
  created_ts    TIMESTAMP,
  update_ts     TIMESTAMP
) USING DELTA;


CREATE TABLE IF NOT EXISTS d_product (
  product_id    STRING,
  product_name  STRING,
  category      STRING,
  unit_price    DECIMAL(10,2),
  price_tier    STRING,
  created_ts    TIMESTAMP,
  update_ts     TIMESTAMP
) USING DELTA;


CREATE TABLE IF NOT EXISTS d_date (
  date_id       DATE,
  day           INT,
  month         INT,
  month_name    STRING,
  quarter       INT,
  year          INT,
  day_of_week   STRING,
  created_ts    TIMESTAMP,
  update_ts     TIMESTAMP
) USING DELTA;


CREATE TABLE IF NOT EXISTS f_order (
  order_id      STRING,
  customer_id   STRING,
  order_date    DATE,
  order_status  STRING,
  order_value   DECIMAL(12,2),
  created_ts    TIMESTAMP,
  update_ts     TIMESTAMP
) USING DELTA;


CREATE TABLE IF NOT EXISTS f_order_line (
  line_item_id      STRING,
  order_id          STRING,
  product_id        STRING,
  order_date        DATE,
  line_status       STRING,
  quantity          INT,
  unit_price        DECIMAL(10,2),
  line_total_price  DECIMAL(12,2),
  created_ts        TIMESTAMP,
  update_ts         TIMESTAMP
) USING DELTA;


CREATE TABLE IF NOT EXISTS f_order_line_status_history (
  history_id        STRING,
  line_item_id      STRING,
  order_id          STRING,
  product_id        STRING,
  status_from       STRING,
  status_to         STRING,
  change_date       DATE,
  changed_by        STRING,
  duration_minutes  INT,
  created_ts        TIMESTAMP,
  update_ts         TIMESTAMP
) USING DELTA;