package com.rustam.kafka_consumer.repository;

import com.rustam.kafka_consumer.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User,Long> {
}
