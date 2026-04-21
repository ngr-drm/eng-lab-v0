package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
public class HealthCheckScheduler {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final PaymentProcessorClient processorClient;
    private final ReactiveRedisTemplate<String, String> redis;

    private final AtomicReference<PaymentDTO.ServiceHealth> healthDefault =
            new AtomicReference<>(new PaymentDTO.ServiceHealth(false, 0));
    private final AtomicReference<PaymentDTO.ServiceHealth> healthFallback =
            new AtomicReference<>(new PaymentDTO.ServiceHealth(false, 0));

    private static final String LEADER_KEY_DEFAULT = "health:leader:default";
    private static final String LEADER_KEY_FALLBACK = "health:leader:fallback";
    private static final Duration LEADER_TTL = Duration.ofSeconds(6);

    private final AtomicBoolean defaultLeader = new AtomicBoolean(false);
    private final AtomicBoolean fallbackLeader = new AtomicBoolean(false);

    /**
     * Leader election: only one instance polls each processor's health.
     * Uses Redis SETNX with TTL to elect leader. Non-leaders read cached health from Redis.
     */
    @Scheduled(fixedDelay = 5000)
    public void checkDefault() {
        tryAcquireAndPoll(LEADER_KEY_DEFAULT, PaymentDTO.ProcessorType.DEFAULT,
                healthDefault, defaultLeader);
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2500)
    public void checkFallback() {
        tryAcquireAndPoll(LEADER_KEY_FALLBACK, PaymentDTO.ProcessorType.FALLBACK,
                healthFallback, fallbackLeader);
    }

    private void tryAcquireAndPoll(String leaderKey, PaymentDTO.ProcessorType type,
                                    AtomicReference<PaymentDTO.ServiceHealth> ref,
                                    AtomicBoolean isLeader) {
        redis.opsForValue()
                .setIfAbsent(leaderKey, "1", LEADER_TTL)
                .subscribe(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        isLeader.set(true);
                        // We are leader — poll processor health
                        processorClient.checkHealth(type)
                                .subscribe(
                                        h -> {
                                            ref.set(h);
                                            // Share health state via Redis for non-leader instances
                                            String healthKey = "health:state:" + type.name().toLowerCase();
                                            String val = h.failing() + ":" + h.minResponseTime();
                                            redis.opsForValue().set(healthKey, val, LEADER_TTL).subscribe();
                                            log.debug("{} health (leader): failing={}, minResponseTime={}ms",
                                                    type, h.failing(), h.minResponseTime());
                                        },
                                        err -> {
                                            if (!(err instanceof PaymentProcessorClient.RateLimitException)) {
                                                ref.set(new PaymentDTO.ServiceHealth(true, 9999));
                                                log.warn("{} health check failed: {}", type, err.getMessage());
                                            }
                                        }
                                );
                    } else {
                        isLeader.set(false);
                        // Not leader — read cached health from Redis
                        String healthKey = "health:state:" + type.name().toLowerCase();
                        redis.opsForValue().get(healthKey)
                                .subscribe(val -> {
                                    if (val != null) {
                                        String[] parts = val.split(":");
                                        boolean failing = Boolean.parseBoolean(parts[0]);
                                        int minResp = Integer.parseInt(parts[1]);
                                        ref.set(new PaymentDTO.ServiceHealth(failing, minResp));
                                    }
                                });
                    }
                });
    }

    public PaymentDTO.ServiceHealth getHealth(PaymentDTO.ProcessorType type) {
        return type == PaymentDTO.ProcessorType.DEFAULT ? healthDefault.get() : healthFallback.get();
    }

    public boolean isDefaultPreferred() {
        PaymentDTO.ServiceHealth def = healthDefault.get();
        if (def.failing()) return false;
        return def.minResponseTime() < 300;
    }
}
