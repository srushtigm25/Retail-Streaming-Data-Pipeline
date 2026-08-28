# Gold layer data model — star schema

```
                     ┌───────────────────┐
                     │   dim_customer     │
                     │───────────────────│
                     │ customer_key (PK) │
                     │ customer_id       │
                     │ customer_name     │
                     │ customer_region   │
                     │ start_date        │  Type-2 SCD:
                     │ end_date          │  one row per version
                     │ is_current        │  of a customer's region
                     └─────────┬─────────┘
                               │
┌───────────────────┐         │        ┌───────────────────┐
│    dim_product     │        │        │     dim_date       │
│───────────────────│        │        │───────────────────│
│ product_key (PK)  │        │        │ date_key (PK)      │
│ product_id        │        │        │ full_date           │
│ product_name      │        │        │ year / month / day   │
│ product_category  │        │        │ day_of_week          │
└─────────┬──────────┘        │        │ is_weekend            │
          │                   │        └──────────┬────────────┘
          │                   │                    │
          │         ┌─────────▼──────────┐         │
          └────────▶│    fact_orders      │◀────────┘
                     │────────────────────│
                     │ order_id           │
                     │ event_id           │
                     │ customer_key (FK)  │
                     │ product_key  (FK)  │
                     │ date_key     (FK)  │
                     │ quantity           │
                     │ unit_price         │
                     │ order_total        │
                     │ status             │
                     │ event_ts           │
                     └────────────────────┘
        grain: one row per order line item
```

## Why star schema (and not 3NF or Data Vault) for this gold layer

**Grain decision:** `fact_orders` is grained at one row per order line item,
not per order. An order with 3 products is 3 fact rows sharing an
`order_id`. This is the classic Kimball trade-off — coarser grain (one row
per order) is smaller and simpler but can't answer "revenue by product
category," which is the first question any retail stakeholder asks. Line-
item grain costs more rows but keeps every downstream aggregation possible
without re-deriving anything.

**Why `dim_customer` is Type-2 SCD and `dim_product` is Type-1:**
Region is a customer attribute the business cares about historically —
"what was Q1 revenue by region" needs to reflect where the customer *was*
when they ordered, not where they live today, or a customer who moved
West→East mid-quarter silently reshuffles historical numbers every time
they move. Product category, by contrast, is treated as always-current here
— if a SKU gets recategorized, the business wants historical reports to
reflect the *current* taxonomy, not an ever-multiplying set of "as of the
time" category snapshots. That's a judgment call I'd confirm with the
analytics team on a real project, not a default I'd assume silently.

**Why not straight to 3NF (normalized OLTP-style schema) for gold:**
3NF minimizes redundancy, which matters for the *source* system writing one
order at a time, but it's the wrong optimization target for a table BI
tools scan and aggregate across millions of rows — every normalized join
is a shuffle in Spark / a join in Trino that the star schema's
denormalized dimensions avoid.

**Why not Data Vault for gold (kept in silver's design space instead):**
Data Vault (Hub/Link/Satellite) optimizes for insert-only auditability and
parallel loading from many divergent source systems — exactly the shape of
problem I *don't* have here (one source: the checkout service's order
events). I'd reach for Data Vault as an intermediate layer if this lake had
to reconcile order data from three merged companies' separate order
systems with different schemas and update cadences; forcing every BI
consumer to join through Hub/Link/Satellite tables for a single-source
retail feed just pushes complexity onto every downstream query for no
benefit here.
