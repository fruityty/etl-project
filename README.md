# etl-project
orders (order_id)
   ↓
order_items (order_id, product_id)
   ↓
products (product_id)

orders (customer_id)
   ↓
customers (customer_id)

orders (order_id)
   ↓
payments (order_id)