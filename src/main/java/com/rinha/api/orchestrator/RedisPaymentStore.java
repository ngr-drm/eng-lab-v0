package com.rinha.api.orchestrator;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * Single ZSET ledger per processor (`payments:default:zset`, `payments:fallback:zset`).
 * Score  = epochMillis
 * Member = "<cid>:<amount>"  -> naturally idempotent.
 *
 * Distributed idempotency via SETNX lock:idem:{cid} (TTL 30s) — prevents two instances
 * from sending the same payment to the processor concurrently.
 */
@Component
public class RedisPaymentStore {

    private static final Duration IDEM_LOCK_TTL = Duration.ofSeconds(30);

    private final StringRedisTemplate redis;

    public RedisPaymentStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private static String zsetKey(PaymentDTO.ProcessorType type) {
        return "payments:" + (type == PaymentDTO.ProcessorType.DEFAULT ? "default" : "fallback") + ":zset";
    }

    /**
     * Try to acquire processing lock for cid. Returns true if acquired (first to process).
     * Returns false if another worker is already handling this cid.
     */
    public boolean tryAcquire(String correlationId) {
        Boolean acquired = redis.opsForValue()
                .setIfAbsent("lock:idem:" + correlationId, "1", IDEM_LOCK_TTL);
        return Boolean.TRUE.equals(acquired);
    }

    public void savePayment(PaymentDTO.ProcessorType type, String correlationId,
                            BigDecimal amount, Instant requestedAt) {
        String zkey = zsetKey(type);
        String member = correlationId + ":" + amount.toPlainString();
        double score = requestedAt.toEpochMilli();
        redis.opsForZSet().add(zkey, member, score);
    }

    public PaymentDTO.SummaryEntry getSummary(PaymentDTO.ProcessorType type, Instant from, Instant to) {
        String zkey = zsetKey(type);
        double minScore = from != null ? from.toEpochMilli() : Double.NEGATIVE_INFINITY;
        double maxScore = to   != null ? to.toEpochMilli()   : Double.POSITIVE_INFINITY;

        Set<String> members = redis.opsForZSet()
                .rangeByScore(zkey, minScore, maxScore);

        long count = 0;
        BigDecimal total = BigDecimal.ZERO;
        if (members != null) {
            for (String m : members) {
                count++;
                total = total.add(extractAmount(m));
            }
        }
        return new PaymentDTO.SummaryEntry(count, total);
    }

    private BigDecimal extractAmount(String member) {
        int idx = member.lastIndexOf(':');
        if (idx < 0) return BigDecimal.ZERO;
        try { return new BigDecimal(member.substring(idx + 1)); }
        catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }

    /** Used by /purge-payments to reset state between official k6 runs. */
    public void purge() {
        redis.delete(java.util.List.of(
                zsetKey(PaymentDTO.ProcessorType.DEFAULT),
                zsetKey(PaymentDTO.ProcessorType.FALLBACK)
        ));
    }
}
