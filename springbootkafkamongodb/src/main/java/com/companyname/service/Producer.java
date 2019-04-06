package com.companyname.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "users";
    private static final String EMPLOYEE_TOPIC = "employee-topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        logger.info(String.format("#### -> Producing message -> %s", message));
        this.kafkaTemplate.send(TOPIC, message);
    }
    public void sendJsonMessage(String jsonmessage) {
        logger.info(String.format("#### -> Producing json message -> %s", jsonmessage));
        this.kafkaTemplate.send(EMPLOYEE_TOPIC, jsonmessage);
    }
}