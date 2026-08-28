package com.lakehouse.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

/**
 * Queries the gold star schema via Trino/JDBC.
 *
 * Why Trino instead of embedding a Spark session in the API: Spark's
 * startup cost (seconds) and executor overhead make it a poor fit for a
 * request/response API expected to answer in tens of milliseconds. Trino
 * is built for exactly this -- low-latency interactive SQL directly over
 * the same lake files Spark wrote (no separate serving-layer copy of the
 * data, no dual-write consistency problem). If sub-10ms p99 mattered here
 * I'd add a small Redis/materialized-view layer refreshed by the Airflow
 * DAG instead of hitting Trino per request; the @Cacheable below is the
 * lightweight version of that.
 */
@Repository
public class SalesRepository {

    private final JdbcTemplate jdbcTemplate;

    public SalesRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final String SALES_SUMMARY_SQL = """
        SELECT dc.customer_region,
               dp.product_category,
               ROUND(SUM(f.order_total), 2) AS revenue,
               COUNT(*) AS order_count
        FROM delta.gold.fact_orders f
        JOIN delta.gold.dim_customer dc
          ON f.customer_key = dc.customer_key AND dc.is_current = true
        JOIN delta.gold.dim_product dp
          ON f.product_key = dp.product_key
        WHERE f.status NOT IN ('CANCELLED', 'REFUNDED')
        GROUP BY dc.customer_region, dp.product_category
        ORDER BY revenue DESC
        """;

    public List<SalesSummary> getSalesSummary() {
        return jdbcTemplate.query(SALES_SUMMARY_SQL, (rs, rowNum) -> new SalesSummary(
                rs.getString("customer_region"),
                rs.getString("product_category"),
                rs.getBigDecimal("revenue"),
                rs.getLong("order_count")
        ));
    }

    public List<SalesSummary> getSalesSummaryByRegion(String region) {
        String sql = SALES_SUMMARY_SQL.replace(
                "WHERE f.status NOT IN ('CANCELLED', 'REFUNDED')",
                "WHERE f.status NOT IN ('CANCELLED', 'REFUNDED') AND dc.customer_region = ?"
        );
        return jdbcTemplate.query(sql, (rs, rowNum) -> new SalesSummary(
                rs.getString("customer_region"),
                rs.getString("product_category"),
                rs.getBigDecimal("revenue"),
                rs.getLong("order_count")
        ), region);
    }

    public BigDecimal getCustomerLifetimeValue(String customerId) {
        String sql = """
            SELECT COALESCE(SUM(f.order_total), 0) AS ltv
            FROM delta.gold.fact_orders f
            JOIN delta.gold.dim_customer dc ON f.customer_key = dc.customer_key
            WHERE dc.customer_id = ? AND f.status NOT IN ('CANCELLED', 'REFUNDED')
            """;
        List<BigDecimal> result = jdbcTemplate.query(sql,
                (rs, rowNum) -> rs.getBigDecimal("ltv"), customerId);
        return result.isEmpty() ? BigDecimal.ZERO : result.get(0);
    }
}
