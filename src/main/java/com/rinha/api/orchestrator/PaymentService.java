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

        PaymentDTO.ProcessorType primary   = healthScheduler.isDefaultPreferred()
                ? PaymentDTO.ProcessorType.DEFAULT
                : PaymentDTO.ProcessorType.FALLBACK;
        PaymentDTO.ProcessorType secondary = primary == PaymentDTO.ProcessorType.DEFAULT
                ? PaymentDTO.ProcessorType.FALLBACK
                : PaymentDTO.ProcessorType.DEFAULT;

        return tryProcess(primary, pReq, req, requestedAt)
                .onErrorResume(ProcessorException.class, e -> {
                    log.warn("Primary {} failed for {}, trying secondary", primary, req.correlationId());
                    return tryProcess(secondary, pReq, req, requestedAt);
                });
    }

    /**
     * Calls processor first, then records in Redis atomically.
     * Wraps processor errors in ProcessorException to distinguish from Redis errors.
     */
    private Mono<Void> tryProcess(PaymentDTO.ProcessorType type,
                                   PaymentDTO.ProcessorPaymentRequest pReq,
                                   PaymentDTO.PaymentRequest req,
                                   Instant requestedAt) {
        return processorClient.processPayment(type, pReq)
                .onErrorMap(e -> !(e instanceof ProcessorException),
                        e -> new ProcessorException(type, e))
                .then(redisStore.savePayment(type, req.correlationId(), req.amount(), requestedAt));
    }

    public Mono<PaymentDTO.PaymentSummaryResponse> getPaymentsSummary(Instant from, Instant to) {
        return Mono.zip(
                redisStore.getSummary(PaymentDTO.ProcessorType.DEFAULT,  from, to),
                redisStore.getSummary(PaymentDTO.ProcessorType.FALLBACK, from, to)
        ).map(t -> new PaymentDTO.PaymentSummaryResponse(t.getT1(), t.getT2()));
    }

    /**
     * Marker exception to distinguish processor failures from Redis/other errors.
     * Only processor errors trigger fallback to secondary processor.
     */
    public static class ProcessorException extends RuntimeException {
        private final PaymentDTO.ProcessorType type;
        public ProcessorException(PaymentDTO.ProcessorType type, Throwable cause) {
            super("processor-failed-" + type, cause);
            this.type = type;
        }
    }
}
