package com.rinha.api.orchestrator;


import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final HealthCheckScheduler healthScheduler;
    private final PaymentProcessorClient processorClient;
    private final RedisPaymentStore redisStore;

    /**
     * Process payment on virtual thread.
     * No distributed lock — relies on:
     *   - HTTP 422 from processor (idempotent by correlationId)
     *   - ZADD idempotent by member (cid:amount)

     * Returns true if payment was accepted (200 or 422) and saved to ledger.
     * Returns false if it should retry.
     */
    public boolean processPayment(PaymentDTO.QueuedPayment item) {
        // requestedAt is the ADMISSION time (captured once in PaymentQueue.offer) and reused
        // across every retry of the same item. This guarantees that:
        //   - The processor stores requestedAt = T_admission on the first attempt that
        //     succeeds (200).
        //   - Subsequent retries that receive 422 (already processed) ZADD the local ledger
        //     with the SAME T_admission → the ZSET score matches what the processor has.
        //   - If the processor accepted on attempt N-1 but we only confirm via 422 on attempt
        //     N (e.g. network blip on the response path), both sides still carry an identical
        //     timestamp → no audit drift on window boundaries.
        // Differs from the TS reference because our retry path is asynchronous (separate
        // scheduler) instead of in-flight semaphore-bounded single-flight per cid; with
        // per-attempt Instant.now() the ledger score would diverge from the processor's
        // canonical requestedAt on 422-confirmation retries.
        Instant requestedAt = item.requestedAt();
        PaymentDTO.ProcessorType target = healthScheduler.currentRoute();

        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                item.correlationId(), item.amount(), requestedAt.toString());

        boolean accepted = processorClient.processPayment(target, pReq);
        if (!accepted) {
            // Reactive circuit breaker: notify the health scheduler so other workers can
            // switch route immediately, instead of waiting for the next 5s health poll.
            healthScheduler.reportFailure(target);
            return false;
        }

        try {
            redisStore.savePayment(target, item.correlationId(), item.amount(), requestedAt);
            return true;
        } catch (Exception e) {
            log.error("[AUDIT] LEDGER_FAIL cid={} target={} err={}",
                    item.correlationId(), target, e.toString());
            // Processor accepted but ZADD failed — retry will get 422, which is fine
            return false;
        }
    }

    private void saveReconciled(PaymentDTO.ProcessorType type,
                                PaymentDTO.QueuedPayment item,
                                PaymentDTO.ProcessorPaymentResponse resp) {
        try {
            redisStore.savePayment(type, item.correlationId(), item.amount(), item.requestedAt());
            log.info("[AUDIT] RECONCILED cid={} target={}", item.correlationId(), type);
        } catch (Exception e) {
            log.error("[AUDIT] RECONCILE_LEDGER_FAIL cid={} target={} err={}",
                    item.correlationId(), type, e.toString());
        }
    }

    public PaymentDTO.PaymentSummaryResponse getPaymentsSummary(Instant from, Instant to) {
        PaymentDTO.SummaryEntry def = redisStore.getSummary(PaymentDTO.ProcessorType.DEFAULT, from, to);
        PaymentDTO.SummaryEntry fb  = redisStore.getSummary(PaymentDTO.ProcessorType.FALLBACK, from, to);
        return new PaymentDTO.PaymentSummaryResponse(def, fb);
    }
}
