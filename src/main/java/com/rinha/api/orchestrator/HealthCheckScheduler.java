package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
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

    // Reactive circuit breaker: when a worker observes a 5xx/timeout from a processor it
    // calls reportFailure(target), which marks that processor as "in cooldown" locally for
    // REACTIVE_COOLDOWN_MS. While in cooldown, currentRoute() will deflect to the other
    // processor (if it is not also in cooldown). This shortens the worst-case detection
    // window from ~10s (5s poll + 5s local cache) to ~tens of ms — without violating the
    // processor's 1-req/5s rate limit on /service-health.
    private static final long REACTIVE_COOLDOWN_MS = 1500;
    private final AtomicLong defaultCooldownUntil  = new AtomicLong(0L);
    private final AtomicLong fallbackCooldownUntil = new AtomicLong(0L);

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

    /**
     * Routing rules:
     *  1. default failing, fallback healthy          → FALLBACK
     *  2. both failing                               → DEFAULT (log warning, nowhere else to go)
     *  3. default fast (RT < 150ms)                  → DEFAULT (prefer cheaper processor)
     *  4. default slow AND fallback significantly    → FALLBACK (only if fallback is at least 2x faster)
     *     faster
     *  5. else                                       → DEFAULT

     * Hysteresis: threshold at 150ms (vs 120) avoids flip-flopping when default hovers at boundary.
     * Inverted multiplier: only switch to fallback when default.RT > fallback.RT.
     * i.e., default must be at least 2x slower to justify the more expensive processor.
     */
    private PaymentDTO.ProcessorType decide(PaymentDTO.ServiceHealth d, PaymentDTO.ServiceHealth f) {
        if (d.failing()) {
            if (f.failing()) {
                log.warn("[ROUTING] Both processors failing — defaulting to DEFAULT");
                return PaymentDTO.ProcessorType.DEFAULT;
            }
            return PaymentDTO.ProcessorType.FALLBACK;
        }
        if (d.minResponseTime() < 150) return PaymentDTO.ProcessorType.DEFAULT;
        if (!f.failing() && d.minResponseTime() > f.minResponseTime() * 2) {
            return PaymentDTO.ProcessorType.FALLBACK;
        }
        return PaymentDTO.ProcessorType.DEFAULT;
    }

    /** Hot path: returns the current routing target. Local 5s cache; Redis GET on miss.
     *  Applies the reactive cooldown on top of the polled decision to deflect away from
     *  a processor that just failed, without waiting for the next poll cycle. */
    public PaymentDTO.ProcessorType currentRoute() {
        PaymentDTO.ProcessorType base = readBaseRoute();
        long now = System.currentTimeMillis();
        boolean dDown = defaultCooldownUntil.get()  > now;
        boolean fDown = fallbackCooldownUntil.get() > now;
        // Deflect only if the OTHER processor is not also in cooldown — otherwise we'd
        // ping-pong between two known-bad targets. If both are down, stick with the polled
        // decision (which already encodes "both failing → DEFAULT").
        if (base == PaymentDTO.ProcessorType.DEFAULT  && dDown && !fDown) return PaymentDTO.ProcessorType.FALLBACK;
        if (base == PaymentDTO.ProcessorType.FALLBACK && fDown && !dDown) return PaymentDTO.ProcessorType.DEFAULT;
        return base;
    }

    private PaymentDTO.ProcessorType readBaseRoute() {
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

    /** Called by workers on 5xx/timeout from a processor. Cheap, lock-free. */
    public void reportFailure(PaymentDTO.ProcessorType type) {
        long until = System.currentTimeMillis() + REACTIVE_COOLDOWN_MS;
        AtomicLong slot = (type == PaymentDTO.ProcessorType.DEFAULT)
                ? defaultCooldownUntil : fallbackCooldownUntil;
        // Only extend the cooldown, never shrink it (concurrent reporters).
        long cur;
        do {
            cur = slot.get();
            if (until <= cur) return;
        } while (!slot.compareAndSet(cur, until));
    }
}
