package com.rinha.api.orchestrator;


import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentService {
    private final HealthCheckScheduler healthScheduler;
    private final PaymentProcessorClient processorClient;
    private final RedisPaymentStore redisStore;
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    public Mono<Void> processPayment(PaymentDTO.PaymentRequest req) {
        Instant requestedAt = Instant.now();
        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                req.correlationId(),
                req.amount(),
                requestedAt.toString()
        );

        PaymentDTO.ProcessorType primary   = healthScheduler.isDefaultPreferred() ? PaymentDTO.ProcessorType.DEFAULT  : PaymentDTO.ProcessorType.FALLBACK;
        PaymentDTO.ProcessorType secondary = primary == PaymentDTO.ProcessorType.DEFAULT      ? PaymentDTO.ProcessorType.FALLBACK : PaymentDTO.ProcessorType.DEFAULT;

        return processorClient.processPayment(primary, pReq)
                .then(redisStore.savePayment(primary, req.correlationId(), req.amount(), requestedAt))
                .onErrorResume(e -> {
                    log.error("Failed to process payment with primary processor {} for correlationId {}, attempting secondary", primary, req.correlationId(), e);
                    return processorClient.processPayment(secondary, pReq)
                            .then(redisStore.savePayment(secondary, req.correlationId(), req.amount(), requestedAt))
                            .doOnError(e2 -> log.error("Failed to process payment with secondary processor {} for correlationId {}", secondary, req.correlationId(), e2));
                });
    }

    public Mono<PaymentDTO.PaymentSummaryResponse> getPaymentsSummary(Instant from, Instant to) {
        return Mono.zip(
                redisStore.getSummary(PaymentDTO.ProcessorType.DEFAULT,  from, to),
                redisStore.getSummary(PaymentDTO.ProcessorType.FALLBACK, from, to)
        ).map(t -> new PaymentDTO.PaymentSummaryResponse(t.getT1(), t.getT2()));
    }
}
