package io.github.zeralius.orderflow.gateway;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.bucket4j.caffeine.CaffeineProxyManager;
import io.github.bucket4j.distributed.proxy.AsyncProxyManager;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Where the rate-limit buckets live: an in-memory Caffeine cache, one bucket per caller. That is
 * right for one gateway instance. With several instances behind a load balancer each would count on
 * its own; then the buckets move to Redis (Bucket4j has a Redis proxy manager with the same
 * interface).
 */
@Configuration
class RateLimitConfig {

  @Bean
  AsyncProxyManager<String> rateLimitBuckets() {
    return new CaffeineProxyManager<String>(
            Caffeine.newBuilder().maximumSize(100_000), Duration.ofMinutes(1))
        .asAsync();
  }
}
