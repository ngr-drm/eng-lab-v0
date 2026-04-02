package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
public class HealthCheckScheduler {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private PaymentProcessorClient processorClient;

    private final AtomicReference<PaymentDTO.ServiceHealth> healthDefault = new AtomicReference<>(new PaymentDTO.ServiceHealth(false, 0));
    private final AtomicReference<PaymentDTO.ServiceHealth> healthFallback = new AtomicReference<>(new PaymentDTO.ServiceHealth(false, 0));


    @Scheduled(fixedDelay = 5000)
    public void checkDefault() {
        processorClient.checkHealth(PaymentDTO.ProcessorType.DEFAULT)
                .subscribe(
                        h -> {
                            healthDefault.set(h);
                            log.debug("Default health: failing={}, minResponseTime={}ms", h.failing(), h.minResponseTime());
                        },
                        err -> {
                            if (!(err instanceof PaymentProcessorClient.RateLimitException)) {
                                healthDefault.set(new PaymentDTO.ServiceHealth(true, 9999));
                                log.warn("Default health check failed: {}", err.getMessage());
                            }
                        }
                );
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2500)
    public void checkFallback() {
        processorClient.checkHealth(PaymentDTO.ProcessorType.FALLBACK)
                .subscribe(
                        h -> {
                            healthFallback.set(h);
                            log.debug("Fallback health: failing={}, minResponseTime={}ms", h.failing(), h.minResponseTime());
                        },
                        err -> {
                            if (!(err instanceof PaymentProcessorClient.RateLimitException)) {
                                healthFallback.set(new PaymentDTO.ServiceHealth(true, 9999));
                                log.warn("Fallback health check failed: {}", err.getMessage());
                            }
                        }
                );
    }

    public PaymentDTO.ServiceHealth getHealth(PaymentDTO.ProcessorType type) {
        return type == PaymentDTO.ProcessorType.DEFAULT ? healthDefault.get() : healthFallback.get();
    }

    public boolean isDefaultPreferred() {
        PaymentDTO.ServiceHealth def = healthDefault.get();
        PaymentDTO.ServiceHealth fal = healthFallback.get();
        if (def.failing()) return false;
        return def.minResponseTime() < 300;
    }

}
