package com.lakehouse.api;

import org.springframework.cache.CacheManager;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The Airflow DAG's final task (warm_api_cache) POSTs here right after the
 * gold tables finish rebuilding, so the API's response cache doesn't serve
 * yesterday's numbers until a natural TTL expiry -- explicit invalidation
 * beats a short TTL when you know exactly when the underlying data changed.
 */
@RestController
@RequestMapping("/internal/cache")
public class CacheController {

    private final CacheManager cacheManager;

    public CacheController(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @PostMapping("/refresh")
    public String refresh() {
        cacheManager.getCacheNames().forEach(name -> cacheManager.getCache(name).clear());
        return "cache cleared";
    }
}
