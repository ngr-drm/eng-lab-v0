package com.rinha.api.orchestrator;


import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final HealthCheckScheduler healthScheduler;
    private final PaymentProcessorClient processorClient;
    private final RedisPaymentStore redisStore;

    /**
     * Synchronous processing on virtual thread.
     *  1. Acquire distributed idempotency lock (SETNX). If already held -> treat as success.
     *  2. Send to processor on current route.
     *  3. On accepted (200 or 422), commit to ledger (ZADD).
     *  4. Otherwise return false -> worker reschedules with backoff.
     */
    public boolean processPayment(PaymentDTO.QueuedPayment item) {
        // 1. Idempotency guard — prevents two workers/instances from sending the same cid.
        if (!redisStore.tryAcquire(item.correlationId())) {
            log.info("[AUDIT] IDEM_SKIP cid={} (already being processed)", item.correlationId());
            return true;
        }

        Instant requestedAt = item.requestedAt();
        PaymentDTO.ProcessorType target = healthScheduler.currentRoute();

        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                item.correlationId(), item.amount(), requestedAt.toString());

        boolean accepted = processorClient.processPayment(target, pReq);
        if (!accepted) return false;

        try {
            redisStore.savePayment(target, item.correlationId(), item.amount(), requestedAt);
            return true;
        } catch (Exception e) {
            log.error("[AUDIT] LEDGER_FAIL cid={} target={} err={}",
                    item.correlationId(), target, e.toString());
            // Processor accepted; force retry (ZADD is idempotent by member).
            return false;
        }
    }

    public PaymentDTO.PaymentSummaryResponse getPaymentsSummary(Instant from, Instant to) {
        PaymentDTO.SummaryEntry def = redisStore.getSummary(PaymentDTO.ProcessorType.DEFAULT, from, to);
        PaymentDTO.SummaryEntry fb  = redisStore.getSummary(PaymentDTO.ProcessorType.FALLBACK, from, to);
        return new PaymentDTO.PaymentSummaryResponse(def, fb);
    }
}
