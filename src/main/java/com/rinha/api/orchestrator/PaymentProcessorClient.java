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
     * Timeout aligned with reference winner (10s) — avoids client-side aborts on slow but successful calls,
     * which is the root cause of "caixa dois" (double-charge across processors).
     * Returns Mono<Boolean> — true on HTTP 2xx, false otherwise. Never throws on 5xx.
     */
    public Mono<Boolean> processPayment(PaymentDTO.ProcessorType type, PaymentDTO.ProcessorPaymentRequest req) {
        long start = System.currentTimeMillis();
        return clientFor(type)
                .post()
                .uri("/payments")
                .bodyValue(req)
                .retrieve()
                .onStatus(status -> status.value() == 422,
                        resp -> resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(b -> {
                                    log.warn("[AUDIT] DUPLICATE_422 cid={} target={} body={}",
                                            req.correlationId(), type, b);
                                    return Mono.error(new ProcessorDuplicate());
                                }))
                .onStatus(status -> status.is4xxClientError() && status.value() != 422,
                        resp -> resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(b -> {
                                    log.warn("[AUDIT] REJECTED_4XX cid={} target={} status={} body={}",
                                            req.correlationId(), type, resp.statusCode().value(), b);
                                    return Mono.error(new ProcessorFailure("4xx-" + resp.statusCode().value()));
                                }))
                .onStatus(HttpStatusCode::is5xxServerError,
                        resp -> resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(b -> {
                                    log.warn("[AUDIT] SERVER_5XX cid={} target={} status={} body={}",
                                            req.correlationId(), type, resp.statusCode().value(), b);
                                    return Mono.error(new ProcessorFailure("5xx-" + resp.statusCode().value()));
                                }))
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(10))
                .doOnSuccess(v -> log.info("[AUDIT] SUCCESS cid={} target={} latency={}ms",
                        req.correlationId(), type, System.currentTimeMillis() - start))
                .thenReturn(Boolean.TRUE)
                .onErrorResume(e -> {
                    long latency = System.currentTimeMillis() - start;
                    if (e instanceof ProcessorDuplicate) {
                        log.warn("[AUDIT] OUTCOME=DUPLICATE cid={} target={} latency={}ms",
                                req.correlationId(), type, latency);
                    } else if (e instanceof TimeoutException) {
                        log.warn("[AUDIT] OUTCOME=TIMEOUT cid={} target={} latency={}ms",
                                req.correlationId(), type, latency);
                    } else {
                        log.warn("[AUDIT] OUTCOME=ERROR cid={} target={} latency={}ms err={}",
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
