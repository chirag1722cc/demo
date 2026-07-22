package com.uailm.config;

import com.uailm.listener.MemoNotificationSubscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;

@Configuration
public class RedisConfig {

    @Value("${app.redis.notification-channel}")
    private String notificationChannel;

    // ── General purpose template (cache reads/writes + stream XADD) ──────────
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        StringRedisSerializer s = new StringRedisSerializer();
        template.setKeySerializer(s);
        template.setValueSerializer(s);
        template.setHashKeySerializer(s);
        template.setHashValueSerializer(s);
        template.afterPropertiesSet();
        return template;
    }

    // ── Stream listener container — inbound stream from ucldd ─────────────────
    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>>
    streamListenerContainer(RedisConnectionFactory factory) {

        var options = StreamMessageListenerContainer
            .StreamMessageListenerContainerOptions
            .<String, MapRecord<String, String, String>>builder()
            .pollTimeout(Duration.ofSeconds(2))
            .build();

        return StreamMessageListenerContainer.create(factory, options);
    }

    // ── Pub/Sub listener container — fan-out SSE notifications across instances
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory factory,
            MemoNotificationSubscriber subscriber) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        // All instances subscribe to this channel
        // Only the instance holding the SSE emitter will push to Angular
        container.addMessageListener(subscriber, new ChannelTopic(notificationChannel));
        return container;
    }
}
