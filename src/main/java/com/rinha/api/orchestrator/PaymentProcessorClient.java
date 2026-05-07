package com.rinha.api.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

@Component
public class PaymentProcessorClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClient.class);

    private final WebClient defaultClient;
    private final WebClient fallbackClient;

    public PaymentProcessorClient(
            WebClient.Builder builder,
            @Value("${payment.processor.default.url}") String defaultUrl,
            @Value("${payment.processor.fallback.url}") String fallbackUrl) {
        this.defaultClient  = builder.baseUrl(defaultUrl).build();
        this.fallbackClient = builder.baseUrl(fallbackUrl).build();
    }

    private WebClient clientFor(PaymentDTO.ProcessorType type) {
        return type == PaymentDTO.ProcessorType.DEFAULT ? defaultClient : fallbackClient;
    }

    /**
     * Sends payment to the chosen processor.
     *  - 200       -> TRUE (newly accepted)
     *  - 422       -> TRUE (already processed; processor confirms idempotency, safe to commit ledger)
     *  - other 4xx -> FALSE (worker reschedules)
     *  - 5xx/timeout/network -> FALSE (worker reschedules)
     * Timeout 10s aligned with reference winner — avoids client aborts on slow-but-successful calls.
     */
    public Mono<Boolean> processPayment(PaymentDTO.ProcessorType type, PaymentDTO.ProcessorPaymentRequest req) {
        long start = System.currentTimeMillis();
        return clientFor(type)
                .post()
                .uri("/payments")
                .bodyValue(req)
                .retrieve()
                .onStatus(status -> status.value() == 422,
                        resp -> resp.releaseBody().then(Mono.error(new ProcessorDuplicate())))
                .onStatus(status -> status.is4xxClientError() && status.value() != 422,
                        resp -> resp.releaseBody().then(Mono.error(
                                new ProcessorFailure("4xx-" + resp.statusCode().value()))))
                .onStatus(HttpStatusCode::is5xxServerError,
                        resp -> resp.releaseBody().then(Mono.error(
                                new ProcessorFailure("5xx-" + resp.statusCode().value()))))
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(10))
                .thenReturn(Boolean.TRUE)
                .onErrorResume(e -> {
                    long latency = System.currentTimeMillis() - start;
                    if (e instanceof ProcessorDuplicate) {
                        // Treat as success: processor already has this cid; ZADD will be a no-op (same member).
                        log.info("[AUDIT] PROCESSOR_DUPLICATE cid={} target={} latency={}ms",
                                req.correlationId(), type, latency);
                        return Mono.just(Boolean.TRUE);
                    }
                    if (e instanceof TimeoutException) {
                        log.warn("[AUDIT] PROCESSOR_TIMEOUT cid={} target={} latency={}ms",
                                req.correlationId(), type, latency);
                    } else if (e instanceof ProcessorFailure) {
                        log.warn("[AUDIT] PROCESSOR_FAILURE cid={} target={} latency={}ms reason={}",
                                req.correlationId(), type, latency, e.getMessage());
                    } else {
                        log.warn("[AUDIT] PROCESSOR_ERROR cid={} target={} latency={}ms err={}",
                                req.correlationId(), type, latency, e.toString());
                    }
                    return Mono.just(Boolean.FALSE);
                });
    }

    public Mono<PaymentDTO.ServiceHealth> checkHealth(PaymentDTO.ProcessorType type) {
        return clientFor(type)
                .get()
                .uri("/payments/service-health")
                .retrieve()
                .onStatus(status -> status.value() == 429,
                        resp -> Mono.error(new RateLimitException()))
                .bodyToMono(PaymentDTO.ServiceHealth.class)
                .timeout(Duration.ofSeconds(10))
                .onErrorResume(e -> {
                    if (e instanceof RateLimitException) return Mono.error(e);
                    return Mono.just(new PaymentDTO.ServiceHealth(true, 9999));
                });
    }

    public static class RateLimitException extends RuntimeException {
        public RateLimitException() { super("rate-limited"); }
    }

    public static class ProcessorDuplicate extends RuntimeException {
        public ProcessorDuplicate() { super("processor-duplicate-422"); }
        @Override public synchronized Throwable fillInStackTrace() { return this; }
    }

    public static class ProcessorFailure extends RuntimeException {
        public ProcessorFailure() { super("processor-failure"); }
        public ProcessorFailure(String detail) { super("processor-failure:" + detail); }
        @Override public synchronized Throwable fillInStackTrace() { return this; }
    }
}
