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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory bounded queue with Virtual Thread consumers.
 * Flow:
 *   1. Controller.offer(req) → enqueue (~0ns) → 202
 *   2. N VThread workers drain queue continuously
 *   3. Each worker picks items (or batches) and calls PaymentService
 *   4. On shutdown: stop accepting, drain remaining items, then exit
 */
@Component
public class PaymentQueue {
    private static final Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    private static final int QUEUE_CAPACITY = 10_000;
    private static final int WORKER_COUNT = 8;
    private static final int BATCH_SIZE = 16;
    private static final long BATCH_WAIT_MS = 5; // max wait to fill batch

    private final PaymentService paymentService;
    private final BlockingQueue<QueuedPayment> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(WORKER_COUNT);
    private final List<Thread> workers = new ArrayList<>();

    public PaymentQueue(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    record QueuedPayment(PaymentDTO.PaymentRequest request, Instant enqueuedAt) {}

    /**
     * Non-blocking enqueue. Returns false if queue is full (triggers 503).
     */
    public boolean offer(PaymentDTO.PaymentRequest req) {
        if (!running.get()) return false;
        return queue.offer(new QueuedPayment(req, Instant.now()));
    }

    public int size() {
        return queue.size();
    }

    @PostConstruct
    public void start() {
        for (int i = 0; i < WORKER_COUNT; i++) {
            final int workerId = i;
            Thread vt = Thread.ofVirtual()
                    .name("payment-worker-" + workerId)
                    .start(() -> workerLoop(workerId));
            workers.add(vt);
        }
        log.info("PaymentQueue started with {} VThread workers, capacity={}", WORKER_COUNT, QUEUE_CAPACITY);
    }

    /**
     * Graceful shutdown:
     *   1. Stop accepting new items
     *   2. Workers drain remaining items
     *   3. Wait up to 30s for drain to complete
     */
    @PreDestroy
    public void shutdown() {
        log.info("PaymentQueue shutting down. Queue size: {}", queue.size());
        running.set(false);

        // Interrupt workers so they wake from poll()
        workers.forEach(Thread::interrupt);

        try {
            boolean drained = shutdownLatch.await(30, TimeUnit.SECONDS);
            if (!drained) {
                log.error("PaymentQueue drain timed out! {} items lost", queue.size());
            } else {
                log.info("PaymentQueue drained successfully");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("PaymentQueue shutdown interrupted, {} items may be lost", queue.size());
        }
    }

    private void workerLoop(int workerId) {
        List<QueuedPayment> batch = new ArrayList<>(BATCH_SIZE);
        try {
            while (running.get() || !queue.isEmpty()) {
                batch.clear();

                // Block-wait for first item
                QueuedPayment first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);

                // Drain up to BATCH_SIZE - 1 more without blocking
                queue.drainTo(batch, BATCH_SIZE - 1);

                processBatch(batch, workerId);
            }

            // Final drain after running=false
            batch.clear();
            queue.drainTo(batch);
            if (!batch.isEmpty()) {
                log.info("Worker-{} draining {} remaining items", workerId, batch.size());
                processBatch(batch, workerId);
            }
        } catch (InterruptedException e) {
            // Shutdown signal — drain remaining
            batch.clear();
            queue.drainTo(batch);
            if (!batch.isEmpty()) {
                log.info("Worker-{} interrupted, draining {} items", workerId, batch.size());
                processBatch(batch, workerId);
            }
        } finally {
            shutdownLatch.countDown();
        }
    }

    private void processBatch(List<QueuedPayment> batch, int workerId) {
        for (QueuedPayment qp : batch) {
            try {
                // Block the VThread (cheap!) until processing completes
                paymentService.processPayment(qp.request()).block();
            } catch (Exception e) {
                log.error("Worker-{} failed to process correlationId={}: {}",
                        workerId, qp.request().correlationId(), e.getMessage());
            }
        }
    }
}

