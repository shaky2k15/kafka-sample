package com.testing;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class Config {

	@Value("${bootstrap.servers}")
	String bootStrapServers;

	@Bean
	public Map<String,Object> configurations(){
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("bootstrap.servers", bootStrapServers);
		properties.put("acks", "all");
        properties.put("retries", 0);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}
	
	@Bean
	public ProducerFactory<String,String> producerFactory(){
		return new DefaultKafkaProducerFactory<>(configurations());
	}
	
	@Bean
	public KafkaTemplate<String,String> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
}
