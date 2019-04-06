package com.companyname.service;

import java.io.IOException;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
	@Autowired
	private MongoTemplate mongoTemplate;
	private final Logger logger = LoggerFactory.getLogger(Producer.class);

	@KafkaListener(topics = "users", groupId = "group_id")
	public void consume(String message) throws IOException {
		logger.info(String.format("#### -> Consumed message -> %s", message));
	}

	@KafkaListener(topics = "employee-topic", groupId = "mongodbtestgrp")
	public void consumeJsonMessageMongoDB(String message) throws IOException {
		logger.info(String.format("#### -> consumeJsonMessageMongoDB -> %s", message));
		Document doc = Document.parse(message);
		mongoTemplate.insert(doc, "employee");
	}

	@KafkaListener(topics = "employee-topic", groupId = "cassandratestgrp")
	public void consumeJsonMessageCassandra(String message) throws IOException {
		logger.info(String.format("#### -> consumeJsonMessageCassandra -> %s", message));
	}

	@KafkaListener(topics = "employee-topic", groupId = "elasticsearchtestgrp")
	public void consumeJsonMessageElasticSearch(String message) throws IOException {
		logger.info(String.format("#### -> consumeJsonMessageElasticSearch -> %s", message));
	}
}