package com.rinha.api.orchestrator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Single ZSET ledger per processor (`payments:default:zset`, `payments:fallback:zset`).
 * Score = epochMillis; member = "<cid>:<amount>".
 * Idempotency key (`payments:idem:<cid>`) prevents double-counting under retries.
 *
 * Lua script ensures atomic idempotency-check + ZADD: either the entry is added once,
 * or nothing happens — exactly-once accounting.
 */
@Component
public class RedisPaymentStore {

    private final ReactiveRedisTemplate<String, String> redis;

    public RedisPaymentStore(
            @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redis) {
        this.redis = redis;
    }

    // KEYS[1]=idemKey, KEYS[2]=zsetKey
    // ARGV[1]=score, ARGV[2]=member, ARGV[3]=ttlSec
    private static final String SAVE_LUA =
            "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[3]) then " +
            "  redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2]) " +
            "  return 1 " +
            "end " +
            "return 0";

    private static final RedisScript<Long> SAVE_SCRIPT = RedisScript.of(SAVE_LUA, Long.class);

    private static String zsetKey(PaymentDTO.ProcessorType type) {
        return "payments:" + (type == PaymentDTO.ProcessorType.DEFAULT ? "default" : "fallback") + ":zset";
    }

    public Mono<Void> savePayment(PaymentDTO.ProcessorType type, String correlationId,
                                  BigDecimal amount, Instant requestedAt) {
        String idem = "payments:idem:" + correlationId;
        String zkey = zsetKey(type);
        String member = correlationId + ":" + amount.toPlainString();
        String score  = String.valueOf(requestedAt.toEpochMilli());

        return redis.execute(SAVE_SCRIPT,
                        List.of(idem, zkey),
                        List.of(score, member, "3600"))
                .next()
                .then();
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

    // Lua: SCAN + DEL all payments:idem:* keys in batches (non-blocking vs KEYS)
    private static final String PURGE_IDEM_LUA =
            "local cursor = '0' " +
            "repeat " +
            "  local result = redis.call('SCAN', cursor, 'MATCH', 'payments:idem:*', 'COUNT', 500) " +
            "  cursor = result[1] " +
            "  if #result[2] > 0 then redis.call('DEL', unpack(result[2])) end " +
            "until cursor == '0' " +
            "return 1";

    private static final RedisScript<Long> PURGE_IDEM_SCRIPT = RedisScript.of(PURGE_IDEM_LUA, Long.class);

    /** Used by /purge-payments to reset state between official k6 runs. */
    public Mono<Void> purge() {
        return redis.delete(
                zsetKey(PaymentDTO.ProcessorType.DEFAULT),
                zsetKey(PaymentDTO.ProcessorType.FALLBACK)
        )
        .then(redis.execute(PURGE_IDEM_SCRIPT, List.of(), List.of()).then());
    }
}
