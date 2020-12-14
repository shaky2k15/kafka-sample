package com.testing;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping(value = "/rest")
public class ApiController {

    private static final Logger logger = LogManager.getLogger(ApiController.class);
    
	@GetMapping(value = "/consume/{recordCount}")
	public String consume(@PathVariable Integer recordCount) {

		if(recordCount!=null&&recordCount <=0) {
	    	  return "error";
	      }
		
	      Properties props = new Properties();
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("group.id", "consumers");
	      props.put("enable.auto.commit", "false");
	      //props.put("auto.commit.interval.ms", "1000");
	      props.put("max.poll.records",1);
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer",          
	    	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	    	         "org.apache.kafka.common.serialization.StringDeserializer");
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	      String topic = "test";
	      consumer.subscribe(Collections.singletonList(topic));
	      logger.debug("Subscribed to topic " + topic);
	      int msgCount =0;
	      
	      
	      while(recordCount != 0) {
	    	  
	         ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis(100)));
	         logger.debug("Count of records returned from a poll : {} with 100ms timeout  ",records.count());
	            for (ConsumerRecord<String, String> record : records)
	            	logger.debug( "offset = {} , key= {} , value= {}", record.offset(), record.key(), record.value());
	           

	            consumer.commitSync();
	            recordCount --;
	            //pagination or polling fixed number of records not possible in apache kafka
	            
	            //https://stackoverflow.com/questions/60232045/kafka-batch-listener-polling-fixed-numbers-of-records-as-much-as-possible

	      }
	      
	      return "records processed";
	
	}

}

