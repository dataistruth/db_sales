--Most product sold
with prod_line as (
    select p.product_name,sum(l.quantity * l.unit_price) as line_item_cost
    from db_sales.silver.f_order_line l
    left join db_sales.silver.d_product p
    on l.product_id = p.product_id
     where
    date_format(order_date_id, 'yyyy-MM') =
    date_format(add_months(current_date(), -1), 'yyyy-MM')
    and l.line_status = 'DELIVERED'
     and l.__END_AT is null
    group by p.product_name
) , Ranked as (
    select product_name,line_item_cost ,
    row_number() over (order by line_item_cost desc) as rank
    from prod_line )

    select * from Ranked where rank= 1

---Top product sold each month (extra)

with table_prod_line as (
    select p.product_name, sum(l.quantity * l.unit_price) as total_revenue, date_format(l.order_date_id, 'yyyy-MM') as order_month
    from db_sales.silver.f_order_line l
    left join db_sales.silver.d_product p on l.product_id = p.product_id
     where l.line_status = 'DELIVERED'
     and l.__END_AT is null
    group by p.product_name, date_format(l.order_date_id, 'yyyy-MM')
),
RANKED AS
 ( select product_name , order_month ,total_revenue,
row_number() over(partition by order_month order by total_revenue desc) as rank
from table_prod_line )

select * from RANKED where rank = 1 order by order_month,total_revenue desc;

