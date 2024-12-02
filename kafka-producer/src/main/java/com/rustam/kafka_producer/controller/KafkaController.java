package com.rustam.kafka_producer.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
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

    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping(path = "/send")
    public void sendMessage(@RequestParam String message){
        kafkaTemplate.send("consumer",message);
        log.info("send message {}",message);
    }
}
