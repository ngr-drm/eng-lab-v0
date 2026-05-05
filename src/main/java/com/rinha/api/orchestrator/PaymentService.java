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

    /**
     * Sends to the gateway chosen by the shared health route.
     * No per-call cross-processor failover (that would cause "caixa dois" on timeout).
     * On failure, the worker re-enqueues the item with backoff and retries on the
     * route that is current at retry time (which may have shifted to FALLBACK).
     *
     * @return Mono<Boolean> — true if the processor accepted (200) AND ledger write succeeded.
     */
    public Mono<Boolean> processPayment(PaymentDTO.QueuedPayment item) {
        Instant requestedAt = item.requestedAt();
        PaymentDTO.ProcessorType target = healthScheduler.currentRoute();

        log.info("[AUDIT] PROCESSING cid={} target={} requestedAt={} attempt={}",
                item.correlationId(), target, requestedAt, item.attempts());

        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                item.correlationId(),
                item.amount(),
                requestedAt.toString()
        );

        return processorClient.processPayment(target, pReq)
                .flatMap(ok -> {
                    if (!Boolean.TRUE.equals(ok)) {
                        log.warn("[AUDIT] PROCESSOR_REJECTED cid={} target={}", item.correlationId(), target);
                        return Mono.just(Boolean.FALSE);
                    }
                    long scoreMs = requestedAt.toEpochMilli();
                    log.info("[AUDIT] LEDGER_SAVING cid={} target={} scoreMsUsed={}",
                            item.correlationId(), target, scoreMs);
                    return redisStore.savePayment(target, item.correlationId(), item.amount(), requestedAt)
                            .doOnSuccess(v -> log.info("[AUDIT] LEDGER_SAVED cid={} target={}",
                                    item.correlationId(), target))
                            .thenReturn(Boolean.TRUE)
                            .onErrorResume(e -> {
                                log.error("[AUDIT] LEDGER_FAIL cid={} target={} err={}",
                                        item.correlationId(), target, e.toString());
                                return redisStore.savePayment(target, item.correlationId(), item.amount(), requestedAt)
                                        .doOnSuccess(v -> log.info("[AUDIT] LEDGER_RETRY_SAVED cid={} target={}",
                                                item.correlationId(), target))
                                        .thenReturn(Boolean.TRUE)
                                        .onErrorResume(e2 -> {
                                            log.error("[AUDIT] LEDGER_RETRY_FAIL cid={} target={} err={}",
                                                    item.correlationId(), target, e2.toString());
                                            return Mono.just(Boolean.FALSE);
                                        });
                            });
                });
    }

    public Mono<PaymentDTO.PaymentSummaryResponse> getPaymentsSummary(Instant from, Instant to) {
        return Mono.zip(
                redisStore.getSummary(PaymentDTO.ProcessorType.DEFAULT,  from, to),
                redisStore.getSummary(PaymentDTO.ProcessorType.FALLBACK, from, to)
        ).map(t -> new PaymentDTO.PaymentSummaryResponse(t.getT1(), t.getT2()));
    }
}
