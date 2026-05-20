package com.rinha.api.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.util.Optional;

@Component
public class PaymentProcessorClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClient.class);

    private final RestClient defaultClient;
    private final RestClient fallbackClient;
    private final RestClient defaultHealthClient;
    private final RestClient fallbackHealthClient;

    public PaymentProcessorClient(
            @Qualifier("defaultProcessorClient") RestClient defaultClient,
            @Qualifier("fallbackProcessorClient") RestClient fallbackClient,
            @Qualifier("defaultHealthClient") RestClient defaultHealthClient,
            @Qualifier("fallbackHealthClient") RestClient fallbackHealthClient) {
        this.defaultClient = defaultClient;
        this.fallbackClient = fallbackClient;
        this.defaultHealthClient = defaultHealthClient;
        this.fallbackHealthClient = fallbackHealthClient;
    }

    private RestClient clientFor(PaymentDTO.ProcessorType type) {
        return type == PaymentDTO.ProcessorType.DEFAULT ? defaultClient : fallbackClient;
    }

    private RestClient healthClientFor(PaymentDTO.ProcessorType type) {
        return type == PaymentDTO.ProcessorType.DEFAULT ? defaultHealthClient : fallbackHealthClient;
    }

    /**
     * Sends payment to the chosen processor (synchronous, runs on virtual thread).
     *  - 200       -> true (newly accepted)
     *  - 422       -> true (already processed; idempotent, safe to commit ledger)
     *  - other 4xx -> false (worker reschedules)
     *  - 5xx/timeout/network -> false (worker reschedules)
     */
    public boolean processPayment(PaymentDTO.ProcessorType type, PaymentDTO.ProcessorPaymentRequest req) {
        long start = System.currentTimeMillis();
        try {
            clientFor(type)
                    .post()
                    .uri("/payments")
                    .body(req)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (HttpClientErrorException e) {
            long latency = System.currentTimeMillis() - start;
            if (e.getStatusCode().value() == 422) {
                log.info("[AUDIT] PROCESSOR_DUPLICATE cid={} target={} latency={}ms",
                        req.correlationId(), type, latency);
                return true;
            }
            log.warn("[AUDIT] PROCESSOR_FAILURE cid={} target={} latency={}ms status={}",
                    req.correlationId(), type, latency, e.getStatusCode().value());
            return false;
        } catch (ResourceAccessException e) {
            long latency = System.currentTimeMillis() - start;
            log.warn("[AUDIT] PROCESSOR_TIMEOUT cid={} target={} latency={}ms err={}",
                    req.correlationId(), type, latency, e.getMostSpecificCause().toString());
            return false;
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            log.warn("[AUDIT] PROCESSOR_ERROR cid={} target={} latency={}ms",
                    req.correlationId(), type, latency);
            return false;
        }
    }

    /**
     * Reconciliation: check if payment exists in processor via GET /payments/{cid}.
     * Returns the payment details if found, empty if not found or error.
     */
    public Optional<PaymentDTO.ProcessorPaymentResponse> getPayment(PaymentDTO.ProcessorType type, String correlationId) {
        try {
            PaymentDTO.ProcessorPaymentResponse resp = clientFor(type)
                    .get()
                    .uri("/payments/{cid}", correlationId)
                    .retrieve()
                    .body(PaymentDTO.ProcessorPaymentResponse.class);
            if (resp != null) {
                log.info("[AUDIT] RECONCILIATION_FOUND cid={} target={}", correlationId, type);
                return Optional.of(resp);
            }
            return Optional.empty();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 404) {
                return Optional.empty();
            }
            log.warn("[AUDIT] RECONCILIATION_ERROR cid={} target={} status={}",
                    correlationId, type, e.getStatusCode().value());
            return Optional.empty();
        } catch (Exception e) {
            log.warn("[AUDIT] RECONCILIATION_ERROR cid={} target={} err={}",
                    correlationId, type, e.toString());
            return Optional.empty();
        }
    }

    public PaymentDTO.ServiceHealth checkHealth(PaymentDTO.ProcessorType type) {
        try {
            PaymentDTO.ServiceHealth h = healthClientFor(type)
                    .get()
                    .uri("/payments/service-health")
                    .retrieve()
                    .body(PaymentDTO.ServiceHealth.class);
            return h != null ? h : new PaymentDTO.ServiceHealth(true, 9999);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 429) {
                throw new RateLimitException();
            }
            return new PaymentDTO.ServiceHealth(true, 9999);
        } catch (Exception e) {
            return new PaymentDTO.ServiceHealth(true, 9999);
        }
    }

    public static class RateLimitException extends RuntimeException {
        public RateLimitException() { super("rate-limited"); }
    }
}
