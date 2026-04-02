package com.rinha.api.orchestrator;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisPaymentStore {

    @Qualifier("reactiveStringRedisTemplate")
    private final ReactiveRedisTemplate<String, String> redis;

    private String key(PaymentDTO.ProcessorType type, String suffix) {
        return "payments:" + type.name().toLowerCase() + ":" + suffix;
    }

    public Mono<Void> savePayment(PaymentDTO.ProcessorType type, String correlationId,
                                  BigDecimal amount, Instant requestedAt) {
        String idempotencyKey = "payments:idempotency:" + correlationId;

        return redis.opsForValue()
                .setIfAbsent(idempotencyKey, "1", Duration.ofHours(1))
                .flatMap(isNew -> {
                    if (Boolean.FALSE.equals(isNew)) {
                        return Mono.empty();
                    }
                    String countKey  = key(type, "count");
                    String amountKey = key(type, "amount");
                    String timeKey   = key(type, "timeline");
                    String value     = correlationId + ":" + amount.toPlainString();
                    double score     = requestedAt.toEpochMilli();

                    return Mono.zip(
                            redis.opsForValue().increment(countKey),
                            redis.opsForValue().increment(amountKey, amount.doubleValue()),
                            redis.opsForZSet().add(timeKey, value, score)
                    ).then();
                });
    }

    public Mono<PaymentDTO.SummaryEntry> getSummary(PaymentDTO.ProcessorType type, Instant from, Instant to) {
        String countKey  = key(type, "count");
        String amountKey = key(type, "amount");
        String timeKey   = key(type, "timeline");

        if (from == null && to == null) {
            return redis.opsForValue().multiGet(List.of(countKey, amountKey))
                    .map(values -> {
                        long count = parseLong(values.get(0));
                        BigDecimal total = parseBigDecimal(values.get(1));
                        return new PaymentDTO.SummaryEntry(count, total);
                    });
        }

        double minScore = from != null ? from.toEpochMilli() : Double.NEGATIVE_INFINITY;
        double maxScore = to   != null ? to.toEpochMilli()   : Double.POSITIVE_INFINITY;

        return redis.opsForZSet()
                .rangeByScore(timeKey, org.springframework.data.domain.Range.closed(minScore, maxScore))
                .collectList()
                .map(entries -> {
                    long count = entries.size();
                    BigDecimal total = entries.stream()
                            .map(v -> {
                                String[] parts = v.split(":", 2);
                                return parts.length == 2 ? new BigDecimal(parts[1]) : BigDecimal.ZERO;
                            })
                            .reduce(BigDecimal.ZERO, BigDecimal::add);
                    return new PaymentDTO.SummaryEntry(count, total);
                });
    }

    private long parseLong(String v) {
        if (v == null || v.isBlank()) return 0L;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return 0L; }
    }

    private BigDecimal parseBigDecimal(String v) {
        if (v == null || v.isBlank()) return BigDecimal.ZERO;
        try { return new BigDecimal(v); } catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }
}
