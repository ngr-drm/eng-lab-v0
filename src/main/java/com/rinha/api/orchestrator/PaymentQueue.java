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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory bounded queue with virtual-thread workers.
 *  - Few workers (2) to limit concurrency on processors and avoid race/contention.
 *  - On failure, item is re-enqueued with exponential backoff (1s, 2s, 4s, 8s, 15s cap),
 *    up to MAX_ATTEMPTS, before being declared a permanent failure.
 *  - Re-enqueue uses a scheduled executor; never blocks the worker on backoff.
 */
@Component
public class PaymentQueue {
    private static final Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    private static final int  QUEUE_CAPACITY = 20_000;
    private static final int  WORKER_COUNT   = 2;
    private static final int  MAX_ATTEMPTS   = 5;
    private static final long BACKOFF_CAP_MS = 15_000;

    private final PaymentService paymentService;
    private final BlockingQueue<PaymentDTO.QueuedPayment> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(WORKER_COUNT);
    private final List<Thread> workers = new ArrayList<>();
    private final ScheduledExecutorService backoffScheduler =
            new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "payment-backoff");
                t.setDaemon(true);
                return t;
            });

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
            // Wait for graceful drain (best effort).
            if (!shutdownLatch.await(30, TimeUnit.SECONDS)) {
                log.error("PaymentQueue drain timeout. lost~={}", queue.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            backoffScheduler.shutdownNow();
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

    private void handle(PaymentDTO.QueuedPayment item, int workerId) {
        try {
            Boolean ok = paymentService.processPayment(item).block();
            if (Boolean.TRUE.equals(ok)) return;
            scheduleRetry(item, workerId);
        } catch (Exception e) {
            log.warn("[AUDIT] WORKER_ERROR worker-{} cid={}: {}", workerId, item.correlationId(), e.toString());
            scheduleRetry(item, workerId);
        }
    }

    private void scheduleRetry(PaymentDTO.QueuedPayment item, int workerId) {
        int next = item.attempts() + 1;
        if (next >= MAX_ATTEMPTS) {
            log.error("[AUDIT] PERMANENT_FAILURE cid={} attempts={}", item.correlationId(), next);
            return;
        }
        long delay = Math.min(1000L * (1L << item.attempts()), BACKOFF_CAP_MS);
        PaymentDTO.QueuedPayment retry = new PaymentDTO.QueuedPayment(
                item.correlationId(), item.amount(), item.requestedAt(), next);
        backoffScheduler.schedule(() -> {
            if (!queue.offer(retry)) {
                log.error("[AUDIT] RETRY_DROPPED cid={} attempt={} (queue full)",
                        retry.correlationId(), next);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
}
