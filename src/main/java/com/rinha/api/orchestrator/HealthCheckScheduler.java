package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health polling + routing decision (synchronous, runs on virtual thread).
 * Single leader (per Redis lock) polls /payments/service-health for both processors every 5s,
 * writes routing decision to Redis (TTL 6s). Non-leaders read from Redis; all instances have a 5s local cache.
 */
@Component
@RequiredArgsConstructor
public class HealthCheckScheduler {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final PaymentProcessorClient processorClient;
    private final StringRedisTemplate redis;

    private static final String LEADER_KEY = "health:leader";
    private static final String ROUTE_KEY  = "health:route";
    private static final Duration LEADER_TTL = Duration.ofSeconds(6);
    private static final Duration ROUTE_TTL  = Duration.ofSeconds(6);
    private static final long LOCAL_CACHE_TTL_MS = 5000;

    private final String instanceId = UUID.randomUUID().toString();

    private final AtomicReference<CachedRoute> localRoute =
            new AtomicReference<>(new CachedRoute(PaymentDTO.ProcessorType.DEFAULT, 0L));

    private record CachedRoute(PaymentDTO.ProcessorType processor, long ts) {}

    @Scheduled(fixedDelay = 5000, initialDelay = 100)
    public void pollAndDecide() {
        try {
            Boolean acquired = redis.opsForValue()
                    .setIfAbsent(LEADER_KEY, instanceId, LEADER_TTL);
            if (!Boolean.TRUE.equals(acquired)) return;

            PaymentDTO.ServiceHealth d = safeCheck(PaymentDTO.ProcessorType.DEFAULT);
            PaymentDTO.ServiceHealth f = safeCheck(PaymentDTO.ProcessorType.FALLBACK);
            PaymentDTO.ProcessorType chosen = decide(d, f);

            if (log.isDebugEnabled()) {
                log.debug("route={} default(failing={},rt={}) fallback(failing={},rt={})",
                        chosen, d.failing(), d.minResponseTime(), f.failing(), f.minResponseTime());
            }
            redis.opsForValue().set(ROUTE_KEY, chosen.name(), ROUTE_TTL);
        } catch (Exception e) {
            log.warn("health poll error: {}", e.toString());
        }
    }

    private PaymentDTO.ServiceHealth safeCheck(PaymentDTO.ProcessorType t) {
        try { return processorClient.checkHealth(t); }
        catch (Exception e) { return new PaymentDTO.ServiceHealth(true, 9999); }
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

    /** Hot path: returns the current routing target. Local 5s cache; Redis GET on miss. */
    public PaymentDTO.ProcessorType currentRoute() {
        CachedRoute c = localRoute.get();
        long now = System.currentTimeMillis();
        if (now - c.ts() < LOCAL_CACHE_TTL_MS) {
            return c.processor();
        }
        try {
            String val = redis.opsForValue().get(ROUTE_KEY);
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
