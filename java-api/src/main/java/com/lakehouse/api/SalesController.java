package com.lakehouse.api;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

/**
 * Downstream teams (the marketing dashboard, the recommendation service)
 * consume the lakehouse through this API rather than querying Delta/Trino
 * directly -- gives us a stable contract even if the gold schema changes,
 * and a place to enforce auth/rate limits that a raw SQL endpoint wouldn't
 * have.
 */
@RestController
@RequestMapping("/api/v1")
public class SalesController {

    private final SalesRepository salesRepository;

    public SalesController(SalesRepository salesRepository) {
        this.salesRepository = salesRepository;
    }

    @GetMapping("/sales-summary")
    @Cacheable("salesSummary")
    public List<SalesSummary> salesSummary() {
        return salesRepository.getSalesSummary();
    }

    @GetMapping("/sales-summary/{region}")
    @Cacheable(value = "salesSummaryByRegion", key = "#region")
    public List<SalesSummary> salesSummaryByRegion(@PathVariable String region) {
        return salesRepository.getSalesSummaryByRegion(region);
    }

    @GetMapping("/customers/{customerId}/lifetime-value")
    public BigDecimal customerLifetimeValue(@PathVariable String customerId) {
        return salesRepository.getCustomerLifetimeValue(customerId);
    }
}
