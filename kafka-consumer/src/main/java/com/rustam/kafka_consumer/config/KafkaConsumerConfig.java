package com.rustam.kafka_consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaLocalHost;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private Integer period;

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
        factory.setCommonErrorHandler(errorHandler());
        return factory;
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
//        errorMap.put(NullPointerException.class,false);
//        return new SimpleRetryPolicy(3,errorMap,true,true)
//    }


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
    public DefaultErrorHandler errorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(period, 3);
        return new DefaultErrorHandler(fixedBackOff);
    }
}
