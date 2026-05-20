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
     *
     * Returns true if payment was accepted (200 or 422) and saved to ledger.
     * Returns false if should retry.
     */
    public boolean processPayment(PaymentDTO.QueuedPayment item) {
        Instant requestedAt = item.requestedAt();
        PaymentDTO.ProcessorType target = healthScheduler.currentRoute();

        PaymentDTO.ProcessorPaymentRequest pReq = new PaymentDTO.ProcessorPaymentRequest(
                item.correlationId(), item.amount(), requestedAt.toString());

        boolean accepted = processorClient.processPayment(target, pReq);
        if (!accepted) {
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

    /**
     * Reconciliation: called when all retries exhausted.
     * Checks if payment exists in either processor via GET /payments/{cid}.
     * If found, saves to ledger to ensure consistency.
     *
     * Returns true if payment was found and reconciled.
     */
    public boolean reconcile(PaymentDTO.QueuedPayment item) {
        String cid = item.correlationId();

        // Try default processor first (cheaper, more likely)
        Optional<PaymentDTO.ProcessorPaymentResponse> resp =
                processorClient.getPayment(PaymentDTO.ProcessorType.DEFAULT, cid);

        if (resp.isPresent()) {
            saveReconciled(PaymentDTO.ProcessorType.DEFAULT, item, resp.get());
            return true;
        }

        // Try fallback processor
        resp = processorClient.getPayment(PaymentDTO.ProcessorType.FALLBACK, cid);
        if (resp.isPresent()) {
            saveReconciled(PaymentDTO.ProcessorType.FALLBACK, item, resp.get());
            return true;
        }

        log.error("[AUDIT] RECONCILIATION_NOT_FOUND cid={} — payment lost", cid);
        return false;
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
