package com.rinha.api.orchestrator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Component
public class RedisPaymentStore {

    private final ReactiveRedisTemplate<String, String> redis;

    public RedisPaymentStore(
            @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redis) {
        this.redis = redis;
    }

    // Lua script: atomic idempotency check + count/amount/timeline write
    // KEYS[1]=idempotencyKey, KEYS[2]=countKey, KEYS[3]=amountKey, KEYS[4]=timelineKey
    // ARGV[1]=amount (string), ARGV[2]=score (epochMillis), ARGV[3]=zsetValue, ARGV[4]=ttl seconds
    // Returns: 1 if saved, 0 if duplicate
    private static final String SAVE_LUA =
            "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end " +
            "redis.call('SET', KEYS[1], '1', 'EX', ARGV[4]) " +
            "redis.call('INCRBY', KEYS[2], 1) " +
            "redis.call('INCRBYFLOAT', KEYS[3], ARGV[1]) " +
            "redis.call('ZADD', KEYS[4], ARGV[2], ARGV[3]) " +
            "return 1";

    private static final RedisScript<Long> SAVE_SCRIPT = RedisScript.of(SAVE_LUA, Long.class);

    private String key(PaymentDTO.ProcessorType type, String suffix) {
        return "payments:" + type.name().toLowerCase() + ":" + suffix;
    }

    /**
     * Atomic save: idempotency check + count + amount + timeline in a single Lua script.
     * Returns Mono.empty() on duplicate, Mono<Void> on success.
     */
    public Mono<Void> savePayment(PaymentDTO.ProcessorType type, String correlationId,
                                  BigDecimal amount, Instant requestedAt) {
        String idempotencyKey = "payments:idempotency:" + correlationId;
        String countKey  = key(type, "count");
        String amountKey = key(type, "amount");
        String timeKey   = key(type, "timeline");
        String value     = correlationId + ":" + amount.toPlainString();
        String score     = String.valueOf(requestedAt.toEpochMilli());
        String ttl       = "3600"; // 1 hour

        List<String> keys = List.of(idempotencyKey, countKey, amountKey, timeKey);
        List<String> args = List.of(amount.toPlainString(), score, value, ttl);

        return redis.execute(SAVE_SCRIPT, keys, args)
                .next()
                .then();
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
