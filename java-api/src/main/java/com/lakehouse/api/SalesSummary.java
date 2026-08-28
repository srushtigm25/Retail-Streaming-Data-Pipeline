package com.lakehouse.api;

import java.math.BigDecimal;

/**
 * Immutable response record for the /api/v1/sales-summary endpoint.
 * One row = one (region, category) revenue rollup, mirroring the gold
 * fact_orders join used in the Spark demo query.
 */
public record SalesSummary(
        String customerRegion,
        String productCategory,
        BigDecimal revenue,
        long orderCount
) {}
