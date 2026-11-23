# Proposed Star Schema Mapping — PROJECT NEXUS

The provided image has `Dim_Customer`, `Dim_Product`, `Dim_Date`, and `Fact_Order`.

| Streaming field | Future dimensional model | Note |
| --- | --- | --- |
| customer_id | Dim_Customer.customer_id → customer_key | Surrogate key assigned during dimension build. |
| product_id | Dim_Product.product_id → product_key | Surrogate key assigned during dimension build. |
| event_time | Dim_Date.full_date → date_key | Choose consistent business timezone; local demo currently UTC. |
| order_id | Fact_Order.order_id | Degenerate dimension. |
| quantity | Fact_Order.quantity | Snapshot after applying updates. |
| unit_price | Proposed Fact_Order.unit_price_at_order | Preserve price at purchase, not mutable catalog price. |
| discount_amount | Fact_Order.discount_amount | Optional event field introduced in the fifth sample micro-batch. |
| event_type | Event history; derive Fact_Order.order_status | Not a direct status without reconciliation policy. |
| event_id, ingest_time | Event audit / lineage | Keep to deduplicate and trace changes. |
| calculated net_amount | Proposed Fact_Order.net_amount | Quantity * purchase price - discount. |

**Grain:** `Fact_Order` should be one row per order line. The current supplied input has one `product_id` per `order_id`; a multi-line future requires `order_line_id` as well.

**Boundary:** The streaming phase writes immutable accepted events; it does not claim to construct final `Fact_Order`. Three `ORDER_UPDATED`/`ORDER_CANCELLED` events target historical `ORD000...` records. To reconcile them accurately, the historical batch must first be loaded and a deterministic version precedence policy defined.
