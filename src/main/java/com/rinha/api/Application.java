package com.rinha.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;

@SpringBootApplication
@EnableScheduling
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean("defaultProcessorClient")
    public RestClient defaultProcessorClient(@Value("${payment.processor.default.url}") String url) {
        return buildPaymentClient(url);
    }

    @Bean("fallbackProcessorClient")
    public RestClient fallbackProcessorClient(@Value("${payment.processor.fallback.url}") String url) {
        return buildPaymentClient(url);
    }

    @Bean("defaultHealthClient")
    public RestClient defaultHealthClient(@Value("${payment.processor.default.url}") String url) {
        return buildHealthClient(url);
    }

    @Bean("fallbackHealthClient")
    public RestClient fallbackHealthClient(@Value("${payment.processor.fallback.url}") String url) {
        return buildHealthClient(url);
    }

    private RestClient buildPaymentClient(String baseUrl) {
        HttpClient httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(2))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(12));

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(factory)
                .build();
    }

    private RestClient buildHealthClient(String baseUrl) {
        HttpClient httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(1))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(1));

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(factory)
                .build();
    }
}
