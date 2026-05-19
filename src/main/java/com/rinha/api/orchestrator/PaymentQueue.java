package com.rinha.api.orchestrator;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory bounded queue with 30 virtual-thread workers.
 *  - On failure, retries inline with Thread.sleep() (cheap on VTs).
 *  - Max 2 attempts, backoff 10s then 20s — conservative to avoid saturating the processor.
 *  - VTs block naturally on queue.poll() and HTTP calls without consuming OS threads.
 */
@Component
public class PaymentQueue {
    private static final Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    private static final int  QUEUE_CAPACITY      = 50_000;
    private static final int  WORKER_COUNT         = 30;
    private static final int  MAX_ATTEMPTS         = 2;
    private static final long INITIAL_BACKOFF_MS   = 10_000; // 10s

    private final PaymentService paymentService;
    private final BlockingQueue<PaymentDTO.QueuedPayment> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(WORKER_COUNT);
    private final List<Thread> workers = new ArrayList<>();

    public PaymentQueue(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Non-blocking enqueue. requestedAt is captured at admission time to anchor audit timeline.
     * Returns false if queue is full (caller should respond 503).
     */
    public boolean offer(PaymentDTO.PaymentRequest req) {
        if (!running.get()) return false;
        return queue.offer(new PaymentDTO.QueuedPayment(
                req.correlationId(), req.amount(), Instant.now(), 0));
    }

    public int size() { return queue.size(); }

    @PostConstruct
    public void start() {
        for (int i = 0; i < WORKER_COUNT; i++) {
            final int id = i;
            Thread vt = Thread.ofVirtual()
                    .name("payment-worker-" + id)
                    .start(() -> workerLoop(id));
            workers.add(vt);
        }
        log.info("PaymentQueue started workers={} capacity={}", WORKER_COUNT, QUEUE_CAPACITY);
    }

    @PreDestroy
    public void shutdown() {
        log.info("PaymentQueue shutting down. queue={}", queue.size());
        running.set(false);
        try {
            if (!shutdownLatch.await(30, TimeUnit.SECONDS)) {
                log.error("PaymentQueue drain timeout. lost~={}", queue.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workers.forEach(Thread::interrupt);
        }
    }

    private void workerLoop(int id) {
        try {
            while (running.get() || !queue.isEmpty()) {
                PaymentDTO.QueuedPayment item;
                try {
                    item = queue.poll(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (!running.get() && queue.isEmpty()) break;
                    continue;
                }
                if (item == null) continue;
                handle(item, id);
            }
        } finally {
            shutdownLatch.countDown();
        }
    }

    /**
     * Process with inline retry. Thread.sleep() on a virtual thread is cheap —
     * it yields the carrier thread, so other workers continue processing.
     */
    private void handle(PaymentDTO.QueuedPayment item, int workerId) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                boolean ok = paymentService.processPayment(item);
                if (ok) return;
            } catch (Exception e) {
                log.warn("[AUDIT] WORKER_ERROR worker-{} cid={} attempt={}: {}",
                        workerId, item.correlationId(), attempt, e.toString());
            }

            if (attempt >= MAX_ATTEMPTS) {
                log.error("[AUDIT] PERMANENT_FAILURE cid={} attempts={}", item.correlationId(), attempt);
                return;
            }

            // Backoff: 10s, 20s (VT sleep is cheap — yields carrier thread)
            long backoff = INITIAL_BACKOFF_MS * attempt;
            log.warn("[AUDIT] RETRY cid={} attempt={}/{} backoff={}ms",
                    item.correlationId(), attempt, MAX_ATTEMPTS, backoff);
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
