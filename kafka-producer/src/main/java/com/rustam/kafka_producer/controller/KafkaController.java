package com.rustam.kafka_producer.controller;

import com.rustam.kafka_producer.dto.UserDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/kafka")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE,makeFinal = true)
@Slf4j
public class KafkaController {


    KafkaTemplate<String,Object> kafkaTemplate;

    @PostMapping(path = "/send")
    public void sendMessage(@RequestParam String message){
        var user = UserDto.builder()
                .id(1L)
                .age(15)
                .name("Rustam")
                .surname("Huseynov")
                .build();
        var pr = new ProducerRecord<String,Object>("consumer",user);
        kafkaTemplate.send(pr);
        log.info("send message key {}",pr.key());
        log.info("send message valur {}",pr.value());
    }
}
