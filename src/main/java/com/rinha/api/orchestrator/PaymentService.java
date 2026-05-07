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
     * Aligned with reference winner:
     *  1. Send to processor on the current route.
     *  2. On confirmed acceptance (200 or 422), save to ledger ONCE.
     *  3. Otherwise, return false so the worker reschedules with backoff.

     * Idempotency is guaranteed by:
     *  - Processor side: 422 on duplicate cid (treated here as TRUE).
     *  - Ledger side: ZSET member = "<cid>:<amount>" — re-save is a no-op.
     *
     * @return Mono<Boolean> — true iff the payment is accounted for end-to-end.
     */
    public Mono<Boolean> processPayment(PaymentDTO.QueuedPayment item) {
        Instant requestedAt = item.requestedAt();
        PaymentDTO.ProcessorType target = healthScheduler.currentRoute();

        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                item.correlationId(),
                item.amount(),
                requestedAt.toString()
        );

        return processorClient.processPayment(target, pReq)
                .flatMap(accepted -> {
                    if (!accepted) {
                        return Mono.just(Boolean.FALSE);
                    }
                    return redisStore.savePayment(target, item.correlationId(), item.amount(), requestedAt)
                            .thenReturn(Boolean.TRUE)
                            .onErrorResume(e -> {
                                log.error("[AUDIT] LEDGER_FAIL cid={} target={} err={}",
                                        item.correlationId(), target, e.toString());
                                // Processor already accepted; force retry to commit ledger
                                // (ZADD is idempotent by member, so a second save is safe).
                                return Mono.just(Boolean.FALSE);
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
