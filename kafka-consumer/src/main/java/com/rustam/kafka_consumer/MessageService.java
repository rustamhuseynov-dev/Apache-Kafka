package com.rustam.kafka_consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {

    private final UserRepository userRepository;
    private final ModelMapper modelMapper;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "consumer",containerFactory = "customKafkaListenerContainerFactory")
    public void saveListener(ConsumerRecord<String,String> data) throws JsonProcessingException {
        User user = new User();
        UserDto userDto = objectMapper.readValue(data.value(), UserDto.class);
        log.info("userDto {}",userDto);
        user.setAge(userDto.getAge());
        user.setName(userDto.getName());
        user.setSurname(userDto.getSurname());
        log.info("user receiver {}",user);
        log.info("message received {}",data.value());
        userRepository.save(user);
    }
}
