package com.rustam.kafka_consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@Slf4j
@EnableKafka
@RequiredArgsConstructor
public class KafkaConsumerApplication {

	private final ObjectMapper objectMapper;

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

	@KafkaListener(topics = "consumer",containerFactory = "customKafkaListenerContainerFactory")
	public void listener(ConsumerRecord<String,String> data) throws JsonProcessingException {
		UserDto userDto = objectMapper.readValue(data.value(), UserDto.class);
		log.info("message received {}",data);
	}
}
