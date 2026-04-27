package com.rinha.api;

import io.netty.channel.ChannelOption;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

@SpringBootApplication
@EnableScheduling
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public WebClient.Builder webClientBuilder() {
		ConnectionProvider provider = ConnectionProvider.builder("custom")
				.maxConnections(32)
				.maxIdleTime(Duration.ofSeconds(20))
				.pendingAcquireMaxCount(512)
				.pendingAcquireTimeout(Duration.ofSeconds(11))
				.build();

		HttpClient httpClient = HttpClient.create(provider)
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
				.option(ChannelOption.TCP_NODELAY, true)
				.responseTimeout(Duration.ofSeconds(11));
		return WebClient.builder()
				.clientConnector(new ReactorClientHttpConnector(httpClient));
	}

	@Bean("reactiveStringRedisTemplate")
	public ReactiveRedisTemplate<String, String> reactiveStringRedisTemplate(
			ReactiveRedisConnectionFactory factory) {
		StringRedisSerializer serializer = new StringRedisSerializer();
		RedisSerializationContext<String, String> context = RedisSerializationContext
				.<String, String>newSerializationContext(serializer)
				.key(serializer)
				.value(serializer)
				.hashKey(serializer)
				.hashValue(serializer)
				.build();
		return new ReactiveRedisTemplate<>(factory, context);
	}
}
