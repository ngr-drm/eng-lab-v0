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


@Component
public class PaymentQueue {
    private static final Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    private static final int QUEUE_CAPACITY = 500;
    private static final int WORKER_COUNT = 20;
    private static final long RETRY_DELAY_MS = 30_000;

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
                req.correlationId(), req.amount(), Instant.now()));
    }

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
            boolean ok = retryScheduler.awaitTermination(5, TimeUnit.SECONDS);
            if (!ok) {
                log.warn("PaymentQueue retry scheduler did not terminate in time.");
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


    private void handle(PaymentDTO.QueuedPayment item, int workerId) {
        try {
            boolean ok = paymentService.processPayment(item);
            if (ok) return;
        } catch (Exception e) {
            log.warn("[AUDIT] WORKER_ERROR worker-{} cid={} details: {}",
                    workerId, item.correlationId(), e.toString());
        }

        PaymentDTO.QueuedPayment retryItem = new PaymentDTO.QueuedPayment(
                item.correlationId(), item.amount(), item.requestedAt());

        retryScheduler.schedule(() -> {
            if (running.get()) {
                boolean offered = queue.offer(retryItem);
                if (!offered) {
                    log.error("[AUDIT] RETRY_QUEUE_FULL cid={}", item.correlationId());
                }
            }
        }, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);

        log.info("[AUDIT] RETRY_SCHEDULED cid={} delay={}ms",
                item.correlationId(), RETRY_DELAY_MS);
    }
}
