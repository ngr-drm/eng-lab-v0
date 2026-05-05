package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health polling + routing decision.
 * Single leader (per Redis lock) polls /payments/service-health for both processors every 5s
 * (respecting the 1-call/5s limit) and writes a routing decision to Redis (TTL 6s).
 * Non-leaders read the decision from Redis. All instances keep a 1s in-memory cache.
 *
 * Routing rules (aligned with reference winner):
 *   - default.failing                                        → FALLBACK
 *   - default.minRT < 120                                    → DEFAULT
 *   - !fallback.failing && fallback.minRT < default.minRT*3  → FALLBACK
 *   - else                                                   → DEFAULT (prefer cheap processor)
 */
@Component
@RequiredArgsConstructor
public class HealthCheckScheduler {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final PaymentProcessorClient processorClient;
    private final ReactiveRedisTemplate<String, String> redis;

    private static final String LEADER_KEY = "health:leader";
    private static final String ROUTE_KEY  = "health:route"; // value: "DEFAULT" or "FALLBACK"
    private static final Duration LEADER_TTL = Duration.ofSeconds(6);
    private static final Duration ROUTE_TTL  = Duration.ofSeconds(6);
    private static final long LOCAL_CACHE_TTL_MS = 5000;

    private final String instanceId = UUID.randomUUID().toString();

    // Local cache: avoid Redis GET per payment.
    private final AtomicReference<CachedRoute> localRoute =
            new AtomicReference<>(new CachedRoute(PaymentDTO.ProcessorType.DEFAULT, 0L));

    private record CachedRoute(PaymentDTO.ProcessorType processor, long ts) {}

    @Scheduled(fixedDelay = 5000, initialDelay = 100)
    public void pollAndDecide() {
        redis.opsForValue()
                .setIfAbsent(LEADER_KEY, instanceId, LEADER_TTL)
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? doLeaderWork()
                        : Mono.empty())
                .subscribe(
                        v -> {},
                        err -> log.warn("health poll error: {}", err.toString())
                );
    }

    private Mono<Void> doLeaderWork() {
        return Mono.zip(
                        processorClient.checkHealth(PaymentDTO.ProcessorType.DEFAULT)
                                .onErrorReturn(new PaymentDTO.ServiceHealth(true, 9999)),
                        processorClient.checkHealth(PaymentDTO.ProcessorType.FALLBACK)
                                .onErrorReturn(new PaymentDTO.ServiceHealth(true, 9999))
                )
                .flatMap(t -> {
                    PaymentDTO.ServiceHealth d = t.getT1();
                    PaymentDTO.ServiceHealth f = t.getT2();
                    PaymentDTO.ProcessorType chosen = decide(d, f);
                    if (log.isDebugEnabled()) {
                        log.debug("route={} default(failing={},rt={}) fallback(failing={},rt={})",
                                chosen, d.failing(), d.minResponseTime(), f.failing(), f.minResponseTime());
                    }
                    return redis.opsForValue().set(ROUTE_KEY, chosen.name(), ROUTE_TTL).then();
                });
    }

    private PaymentDTO.ProcessorType decide(PaymentDTO.ServiceHealth d, PaymentDTO.ServiceHealth f) {
        if (d.failing()) {
            return f.failing() ? PaymentDTO.ProcessorType.DEFAULT : PaymentDTO.ProcessorType.FALLBACK;
        }
        if (d.minResponseTime() < 120) return PaymentDTO.ProcessorType.DEFAULT;
        if (!f.failing() && f.minResponseTime() < d.minResponseTime() * 3) {
            return PaymentDTO.ProcessorType.FALLBACK;
        }
        return PaymentDTO.ProcessorType.DEFAULT;
    }

    /**
     * Hot path: returns the current routing target.
     * Local 5s cache; on miss reads Redis (non-blocking via cache.get only — block-safe on virtual thread).
     */
    public PaymentDTO.ProcessorType currentRoute() {
        CachedRoute c = localRoute.get();
        long now = System.currentTimeMillis();
        if (now - c.ts() < LOCAL_CACHE_TTL_MS) {
            return c.processor();
        }
        // Refresh from Redis (block on VThread is cheap)
        try {
            String val = redis.opsForValue().get(ROUTE_KEY).block(Duration.ofMillis(50));
            PaymentDTO.ProcessorType p = (val == null)
                    ? PaymentDTO.ProcessorType.DEFAULT
                    : PaymentDTO.ProcessorType.valueOf(val);
            localRoute.set(new CachedRoute(p, now));
            return p;
        } catch (Exception e) {
            return c.processor();
        }
    }
}
