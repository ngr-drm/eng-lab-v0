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
 * Health polling + routing decision with reactive circuit breaker.
 *
 * Two layers of routing intelligence:
 *   1. Poll-based (5s): Leader polls /service-health, writes decision to Redis.
 *   2. Reactive (instant): Any worker that receives 5xx/timeout calls reportFailure(),
 *      which puts the processor in local cooldown for 2s. Other workers see the cooldown
 *      immediately and deflect to the healthy processor — no need to wait for the next poll.
 *
 * This reduces the "blindness window" from 5-10s (poll + cache) to <2s in worst case,
 * and to ~0ms in the common case where a worker hits the failure first.
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

    // Reduced from 5s to 2s: faster reaction to Redis route changes
    private static final long LOCAL_CACHE_TTL_MS = 2000;

    // Reactive circuit breaker: cooldown duration after a worker reports failure
    private static final long COOLDOWN_MS = 2000;
    private final AtomicLong defaultCooldownUntil = new AtomicLong(0);
    private final AtomicLong fallbackCooldownUntil = new AtomicLong(0);

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
        if (d.minResponseTime() <= 100) return PaymentDTO.ProcessorType.DEFAULT;
        if (!f.failing() && d.minResponseTime() > f.minResponseTime() * 2) {
            return PaymentDTO.ProcessorType.FALLBACK;
        }
        return PaymentDTO.ProcessorType.DEFAULT;
    }

    /**
     * Hot path: returns the routing target with reactive circuit breaker overlay.

     * Logic:
     *   1. Read the poll-based route (from local cache or Redis)
     *   2. If that processor is in cooldown AND the other is NOT, deflect immediately
     *   3. If both are in cooldown, stick with the polled route (nowhere else to go)

     * This ensures that the first worker to hit a 5xx triggers an instant reroute
     * for all subsequent workers, without waiting for the next 5s poll cycle.
     */
    public PaymentDTO.ProcessorType currentRoute() {
        PaymentDTO.ProcessorType polled = getPolledRoute();
        long now = System.currentTimeMillis();

        boolean defaultInCooldown  = now < defaultCooldownUntil.get();
        boolean fallbackInCooldown = now < fallbackCooldownUntil.get();

        // Deflect only if the OTHER processor is healthy (not in cooldown)
        if (polled == PaymentDTO.ProcessorType.DEFAULT && defaultInCooldown && !fallbackInCooldown) {
            return PaymentDTO.ProcessorType.FALLBACK;
        }
        if (polled == PaymentDTO.ProcessorType.FALLBACK && fallbackInCooldown && !defaultInCooldown) {
            return PaymentDTO.ProcessorType.DEFAULT;
        }
        return polled;
    }

    private PaymentDTO.ProcessorType getPolledRoute() {
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

    /**
     * Called by PaymentService when a processor returns 5xx/timeout.
     * Puts the processor in local cooldown for COOLDOWN_MS (2s).
     * Lock-free (CAS), idempotent (only extends, never shrinks cooldown).
     */
    public void reportFailure(PaymentDTO.ProcessorType type) {
        long cooldownEnd = System.currentTimeMillis() + COOLDOWN_MS;
        AtomicLong target = (type == PaymentDTO.ProcessorType.DEFAULT)
                ? defaultCooldownUntil
                : fallbackCooldownUntil;

        // CAS loop: only extend the cooldown, never shrink it
        long current;
        do {
            current = target.get();
            if (cooldownEnd <= current) return; // already in longer cooldown
        } while (!target.compareAndSet(current, cooldownEnd));
    }
}
