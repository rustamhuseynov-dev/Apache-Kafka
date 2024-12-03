package com.rustam.kafka_consumer.config;

import com.rustam.kafka_consumer.handle.ExceptionHandle;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaLocalHost;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.back.off.period}")
    private Integer period;

    //private final ExceptionHandle exceptionHandle;

    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(){
        var factory = new ConcurrentKafkaListenerContainerFactory<String,Object>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public KafkaListenerContainerFactory<?> customKafkaListenerContainerFactory(){
        var factory = new ConcurrentKafkaListenerContainerFactory<String,Object>();
        factory.setConsumerFactory(customConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    @Bean
    public ConsumerFactory<String,Object> consumerFactory(){
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaLocalHost);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,deserializer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String,Object> customConsumerFactory(){
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaLocalHost);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }


    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        // DLQ Recoverer
        DeadLetterPublishingRecoverer recoverer = deadLetterPublishingRecoverer();

        // Fixed retry siyasəti
        FixedBackOff backOff = new FixedBackOff(period, 5); // 1 saniyə fasilə, 3 dəfə retry

        // DefaultErrorHandler konfiqurasiyası
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class); // Retry edilməyəcək error
        errorHandler.setAckAfterHandle(true); // Error handler-dən sonra ACK et
        return errorHandler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate(),
                (record, ex) -> {
                    if (ex.getCause() instanceof IllegalArgumentException) {
                        // DLQ üçün xüsusi mövzu
                        return new TopicPartition(record.topic() + ".invalid", record.partition());
                    }
                    return new TopicPartition(record.topic() + ".dlq", record.partition()); // Default DLQ
                });
    }

//    @Bean
//    public RetryTemplate retryTemplate(){
//        RetryTemplate retryTemplate = new RetryTemplate();
//        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
//        fixedBackOffPolicy.setBackOffPeriod(period);
//        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
//        retryTemplate.setRetryPolicy(getSimpleRetryPolicy());
//        return retryTemplate;
//    }
//
//    private SimpleRetryPolicy getSimpleRetryPolicy() {
//        HashMap<Class<? extends Throwable>,Boolean> errorMap = new HashMap<>();
//        errorMap.put(NullPointerException.class,true);
//        return new SimpleRetryPolicy(5,errorMap,true,true);
//    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaLocalHost);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

}

