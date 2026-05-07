package com.rinha.api.orchestrator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Single ZSET ledger per processor (`payments:default:zset`, `payments:fallback:zset`).
 * Score  = epochMillis
 * Member = "<cid>:<amount>"  -> naturally idempotent: same cid+amount overwrites the same entry.

 * No local idempotency key: the payment-processor itself is idempotent (returns 422 on duplicate),
 * and the ZSET dedups by member, so a retried save is a no-op for accounting.
 * Aligned with the reference winning solution.
 */
@Component
public class RedisPaymentStore {

    private final ReactiveRedisTemplate<String, String> redis;

    public RedisPaymentStore(
            @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redis) {
        this.redis = redis;
    }

    private static String zsetKey(PaymentDTO.ProcessorType type) {
        return "payments:" + (type == PaymentDTO.ProcessorType.DEFAULT ? "default" : "fallback") + ":zset";
    }

    public Mono<Void> savePayment(PaymentDTO.ProcessorType type, String correlationId,
                                  BigDecimal amount, Instant requestedAt) {
        String zkey = zsetKey(type);
        String member = correlationId + ":" + amount.toPlainString();
        double score = requestedAt.toEpochMilli();
        return redis.opsForZSet().add(zkey, member, score).then();
    }

    public Mono<PaymentDTO.SummaryEntry> getSummary(PaymentDTO.ProcessorType type, Instant from, Instant to) {
        String zkey = zsetKey(type);
        double minScore = from != null ? from.toEpochMilli() : Double.NEGATIVE_INFINITY;
        double maxScore = to   != null ? to.toEpochMilli()   : Double.POSITIVE_INFINITY;

        return redis.opsForZSet()
                .rangeByScore(zkey, org.springframework.data.domain.Range.closed(minScore, maxScore))
                .reduce(new Object[]{0L, BigDecimal.ZERO}, (acc, v) -> {
                    acc[0] = ((Long) acc[0]) + 1L;
                    acc[1] = ((BigDecimal) acc[1]).add(extractAmount(v));
                    return acc;
                })
                .map(acc -> new PaymentDTO.SummaryEntry((Long) acc[0], (BigDecimal) acc[1]));
    }

    private BigDecimal extractAmount(String member) {
        int idx = member.lastIndexOf(':');
        if (idx < 0) return BigDecimal.ZERO;
        try { return new BigDecimal(member.substring(idx + 1)); }
        catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }

    /** Used by /purge-payments to reset state between official k6 runs. */
    public Mono<Void> purge() {
        return redis.delete(
                zsetKey(PaymentDTO.ProcessorType.DEFAULT),
                zsetKey(PaymentDTO.ProcessorType.FALLBACK)
        ).then();
    }
}
