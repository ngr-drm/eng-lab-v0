package com.rinha.api.presentation;


import com.rinha.api.orchestrator.PaymentDTO;
import com.rinha.api.orchestrator.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;

@RestController
@RequiredArgsConstructor
public class PaymentController {
    private final PaymentService paymentService;

    @GetMapping("/health")
    public Mono<ResponseEntity<String>> healthCheck() {
        return Mono.just(ResponseEntity.ok("OK"));
    }

    @PostMapping("/payments")
    public Mono<ResponseEntity<Void>> processPayment(@RequestBody PaymentDTO.PaymentRequest req) {
        return this.paymentService.processPayment(req)
                .thenReturn(ResponseEntity.ok().<Void>build())
                .onErrorReturn(ResponseEntity.status(503).<Void>build());
    }

    @GetMapping("/payments-summary")
    public Mono<ResponseEntity<PaymentDTO.PaymentSummaryResponse>> getSummary(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {

        Instant fromInstant = from != null ? Instant.parse(from) : null;
        Instant toInstant   = to   != null ? Instant.parse(to)   : null;

        return paymentService.getPaymentsSummary(fromInstant, toInstant)
                .map(ResponseEntity::ok);
    }
}
