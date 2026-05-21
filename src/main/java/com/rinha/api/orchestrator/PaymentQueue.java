package com.rinha.api.orchestrator;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory bounded queue with 50 virtual-thread workers.
 * Conservative retry: 1 retry after 15s (respects processor's 10s max response time).
 * On permanent failure, reconciliation via GET /payments/{cid} before giving up.
 */
@Component
public class PaymentQueue {
    private static final Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    private static final int QUEUE_CAPACITY = 50_000;
    private static final int WORKER_COUNT = 15;
    private static final int MAX_ATTEMPTS = 3;  // 1 initial + 2 retries
    private static final long RETRY_DELAY_MS = 5_000; // 5s — faster retry for throughput

    private final PaymentService paymentService;
    private final BlockingQueue<PaymentDTO.QueuedPayment> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "retry-scheduler");
        t.setDaemon(true);
        return t;
    });
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
        log.info("PaymentQueue started workers={} capacity={} retryDelay={}ms",
                WORKER_COUNT, QUEUE_CAPACITY, RETRY_DELAY_MS);
    }

    @PreDestroy
    public void shutdown() {
        log.info("PaymentQueue shutting down. queue={}", queue.size());
        running.set(false);
        retryScheduler.shutdown();
        try {
            if (!shutdownLatch.await(30, TimeUnit.SECONDS)) {
                log.error("PaymentQueue drain timeout. lost~={}", queue.size());
            }
            retryScheduler.awaitTermination(5, TimeUnit.SECONDS);
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
     * Process payment. On failure:
     * - If attempts < MAX_ATTEMPTS: schedule retry after 15s (non-blocking)
     * - If attempts >= MAX_ATTEMPTS: try reconciliation, then give up
     */
    private void handle(PaymentDTO.QueuedPayment item, int workerId) {
        try {
            boolean ok = paymentService.processPayment(item);
            if (ok) return;
        } catch (Exception e) {
            log.warn("[AUDIT] WORKER_ERROR worker-{} cid={} attempt={}: {}",
                    workerId, item.correlationId(), item.attempts() + 1, e.toString());
        }

        int currentAttempt = item.attempts() + 1;

        if (currentAttempt >= MAX_ATTEMPTS) {
            // Exhausted retries — try reconciliation before giving up
            log.warn("[AUDIT] RETRIES_EXHAUSTED cid={} attempts={} — attempting reconciliation",
                    item.correlationId(), currentAttempt);

            boolean reconciled = paymentService.reconcile(item);
            if (!reconciled) {
                log.error("[AUDIT] PERMANENT_FAILURE cid={} attempts={}",
                        item.correlationId(), currentAttempt);
            }
            return;
        }

        // Schedule retry after 15s (non-blocking — worker returns to poll immediately)
        PaymentDTO.QueuedPayment retryItem = new PaymentDTO.QueuedPayment(
                item.correlationId(), item.amount(), item.requestedAt(), currentAttempt);

        retryScheduler.schedule(() -> {
            if (running.get()) {
                boolean offered = queue.offer(retryItem);
                if (!offered) {
                    log.error("[AUDIT] RETRY_QUEUE_FULL cid={}", item.correlationId());
                }
            }
        }, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);

        log.info("[AUDIT] RETRY_SCHEDULED cid={} attempt={}/{} delay={}ms",
                item.correlationId(), currentAttempt, MAX_ATTEMPTS, RETRY_DELAY_MS);
    }
}
