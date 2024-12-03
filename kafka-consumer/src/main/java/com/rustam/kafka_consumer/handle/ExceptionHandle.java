package com.rustam.kafka_consumer.handle;

import com.rustam.kafka_consumer.dto.message.KafkaErrorMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RequiredArgsConstructor
@RestControllerAdvice
public class ExceptionHandle extends DefaultErrorHandler {

    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Override
    public boolean handleOne(Exception exception, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, MessageListenerContainer container) {
        handle(exception, record);
        return false;
    }

    private void handle(Exception exception, ConsumerRecord<?, ?> consumer) {
        Throwable cause = exception.getCause();
        log.error("{}", cause.getMessage());

        KafkaErrorMessage<Object> kafkaErrorMessage = KafkaErrorMessage
                .builder()
                .data(consumer.value())
                .error(exception.getMessage())
                .build();

        kafkaTemplate.send(getTopic(consumer),kafkaErrorMessage);

    }

    private String getTopic(ConsumerRecord<?,?> consumer) {
        return String.format("%s_%s_ERROR",consumer.topic(),groupId);
    }

    @Override
    public boolean isAckAfterHandle() {
        return true;
    }
}
