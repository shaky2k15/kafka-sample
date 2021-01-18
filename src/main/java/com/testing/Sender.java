package com.testing;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Sender {

	@Autowired
	private KafkaTemplate<String,String> kafkaTemplate;
	
	public void sendMessage(String topic, String msg) throws InterruptedException, ExecutionException {
		kafkaTemplate.send(topic, msg);
	}
}
