package com.rustam.kafka_consumer.mapper;

import org.modelmapper.ModelMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Mapper {

    @Bean
    public ModelMapper modelMapper() {
        ModelMapper obj = new ModelMapper();
        return obj;
    }
}