package com.rinha.api.orchestrator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
public class PaymentProcessorClient {

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
        return clientFor(type)
                .post()
                .uri("/payments")
                .bodyValue(req)
                .retrieve()
                .onStatus(HttpStatusCode::isError,
                        resp -> resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(b -> Mono.error(new ProcessorFailure())))
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(10))
                .thenReturn(Boolean.TRUE)
                .onErrorReturn(Boolean.FALSE);
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

    public static class ProcessorFailure extends RuntimeException {
        public ProcessorFailure() { super("processor-failure"); }
        @Override public synchronized Throwable fillInStackTrace() { return this; }
    }
}
