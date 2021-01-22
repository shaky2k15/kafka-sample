package com.testing;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;



@Api(value = "ApiController", description = "Kafka client")
@RestController
@RequestMapping(value = "/rest")
public class ApiController {

	@Value("${max.poll.records}")
	int maxPollRecords;

	@Value("${session.timeout.ms}")
	int sessionTimeoutMs;

	@Value("${max.poll.interval.ms}")
	int maxPollIntervalMs;

	@Value("${consumer.poll.timeout.ms}")
	int consumerPollTimeoutMs;

	@Value("${bootstrap.servers}")
	String bootStrapServers;

	@Value("${enable.auto.commit}")
	String enableAutoCommit;

	@Autowired
	Sender kafkaSender;

	@Autowired
	RestTemplate restTemplate;

	private static final Logger logger = LogManager.getLogger(ApiController.class);

	@ApiOperation(value = "Get records from a topic related to groupId ", response = ResponseEntity.class)
	@GetMapping(value = "/consumers/{groupId}/{topicName}/records")
	@ResponseBody
	public ResponseEntity<String> consume(@PathVariable String groupId, @PathVariable String topicName) throws JsonProcessingException {

		if (groupId == null && topicName == null) {
		//	return "error";
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", bootStrapServers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
		props.put("heartbeat.interval.ms", 10000);
		// props.put("auto.commit.interval.ms", "1000");
		// Records batch size
		props.put("max.poll.records", maxPollRecords);

		// This is a sensitive value related to consumer session currently using default value. 
		//Following error observed with lesser session timeout values.
		/*
		 * Servlet.service() for servlet [dispatcherServlet] in context with path []
		 * threw exception [Request processing failed; nested exception is
		 * org.apache.kafka.common.errors.InvalidSessionTimeoutException: The session
		 * timeout is not within the range allowed by the broker (as configured by
		 * group.min.session.timeout.ms and group.max.session.timeout.ms).] with root
		 * cause
		 */
		props.put("session.timeout.ms", sessionTimeoutMs);

		props.put("max.poll.interval.ms", maxPollIntervalMs);

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList(topicName));
		logger.debug("Subscribed to topic  : {}  with groupId  : {} ", topicName, groupId);
		boolean poll = true;
		List<Object> list = new ArrayList<Object>();
		ObjectMapper objectMapper = new ObjectMapper();


		while (poll) {
			// poll only once
			ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis(consumerPollTimeoutMs)));
			logger.debug("Count of records returned from a poll : {} with  :  {}  timeout  ", records.count(),
					consumerPollTimeoutMs);
			for (ConsumerRecord<String, String> record : records) {
				logger.debug("offset = {} , key= {} , value= {}", record.offset(), record.key(), record.value());
				if(record.value().trim().length()!=0) {
					try{
					list.add(objectMapper.readValue(record.value(), Object.class));
					} catch (Exception e) {
						logger.debug("Ignoring the record with value  {]", record.value());
					}
			    }


			}
			consumer.commitSync();
			poll = false;
			// pagination or polling fixed number of records not possible in apache kafka
			// https://stackoverflow.com/questions/60232045/kafka-batch-listener-polling-fixed-numbers-of-records-as-much-as-possible

			// max.poll.interval.ms interval with subsequent polls
			// https://stackoverflow.com/questions/54948391/kafka-consumer-group-max-poll-interval-ms-not-working
		}
		consumer.close();
        String response =  null;
        response = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(list);
        logger.debug("value {} ",response);
        
		return ResponseEntity.ok(response);


	}
	
	@ApiOperation(value = "Reset offset in a topic related to groupId ", response = ResponseEntity.class)
	@GetMapping(value = "/consumers/{groupId}/{topicName}/reset-offset/{offsetId}")
	@ResponseBody
	public ResponseEntity<String> resetOffset(@PathVariable String groupId, @PathVariable String topicName,@PathVariable String offsetId) throws JsonProcessingException {

		if (groupId == null && topicName == null && offsetId == null) {
			return ResponseEntity.badRequest().body("Input parameters missing");
		}
		
		ProcessBuilder processBuilder = new ProcessBuilder();
		StringBuilder strBuilder = new StringBuilder("kafka-consumer-groups");
		strBuilder.append(" --bootstrap-server ");
		strBuilder.append(bootStrapServers);
		strBuilder.append(" --group ");
		strBuilder.append(groupId);
		strBuilder.append(" --topic ");
		strBuilder.append(topicName);
		strBuilder.append(" --reset-offsets ");
		strBuilder.append(" --to-offset ");
		strBuilder.append(offsetId);
		strBuilder.append(" --execute ");
		processBuilder.command("bash","-c", strBuilder.toString());
		logger.debug("command to be ran in bash {} ",strBuilder.toString());
		StringBuilder responseOutput = null;
		try {
			Process process = processBuilder.start();
			responseOutput = new StringBuilder();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String commandOutput = null;
			while((commandOutput = rdr.readLine()) != null) {
				responseOutput.append(commandOutput);
			}
			
			if(process.waitFor() == 0) {
				logger.debug("Successfully ran the command {} ", strBuilder.toString());
			} else {
				logger.error("Command line processing error for the command {}", strBuilder.toString());
			}
			
		} catch (Exception ex) {
			logger.error("Error during reset offset activity");
			logger.error(ex);
		}
		
		return ResponseEntity.ok().body(responseOutput.toString());


	}



	@ApiOperation(value = "Reset offset in a topic related to groupId using admin client", response = ResponseEntity.class)
	@GetMapping(value = "/adminclient/consumers/{groupId}/partitions/{partitionId}/{topicName}/reset-offset/{offsetId}")
	@ResponseBody
	public ResponseEntity<String> resetOffsetUsingAdminClient(@PathVariable String groupId, @PathVariable String topicName, @PathVariable int partitionId, @PathVariable String offsetId) throws JsonProcessingException {

        // initialize admin client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
        AdminClient admin = AdminClient.create(props);
        
        
        
		if (groupId == null && topicName == null && offsetId == null) {
			return ResponseEntity.badRequest().body("Input parameters missing");
		}
		
        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
        TopicPartition partitionTobeCommitted = new TopicPartition(topicName,partitionId);
        resetOffsets.put(partitionTobeCommitted, new OffsetAndMetadata(Integer.valueOf(offsetId)));
	    String responseContent = "success";
        try {
            admin.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
        } catch (ExecutionException e) {
            logger.error("Failed to update the offsets committed by group  {}  with error {} " , groupId  , e.getMessage());
            if (e.getCause() instanceof UnknownMemberIdException) {
                logger.error("Check if consumer group is still active.");
                }
            responseContent = "failed";
        } catch (Exception e) {
        	logger.error(e);
            responseContent = "failed";

        }
	
		
		return ResponseEntity.ok().body(responseContent);


	}
	

        
	@ApiOperation(value = "Post records to a topic related to groupId ", response = ResponseEntity.class)
	@PostMapping(value = "/producers/{topicName}")
	@ResponseBody
	public ResponseEntity<String> produce( @PathVariable String topicName, @RequestBody String requestBody) throws JsonProcessingException {

		Properties props = new Properties();
		props.put("bootstrap.servers", bootStrapServers);
		props.put("acks", "all");
        props.put("retries", 0);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Instant startProducer = Instant.now();
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		RecordMetadata data = null;
		
		try {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,requestBody);
			data = producer.send(record).get();
            logger.debug("Sending a message to a topic  : {} ", topicName);

		} catch (CancellationException ex) {
			logger.error("CancellationException" ,ex);
		} catch (ExecutionException ex) {
			logger.error("ExecutionException" ,ex);
		} catch (Exception ex) {
			logger.error("Exception" ,ex);
		} finally {
			producer.close();
		}
		Instant stopProducer = Instant.now();
		logger.debug("Time taken to send a message using producer in ms {} ",Duration.between(startProducer, stopProducer).toMillis());
        
		return ResponseEntity.ok(data.toString());


	}


        @ApiOperation(value = "Post records to a topic related to groupId using kafkaTemplate ", response = ResponseEntity.class)
	@PostMapping(value = "/kafkatemplate/producers/{topicName}")
	@ResponseBody
	public ResponseEntity<String> produceUsingKafkaTemplate( @PathVariable String topicName, @RequestBody String requestBody) throws JsonProcessingException {

		Instant startProducer = Instant.now();
		try {
			 kafkaSender.sendMessage(topicName,requestBody);
                         logger.debug("Sending a message to a topic  : {}  with content {} ", topicName , requestBody);

		} catch (CancellationException ex) {
			logger.error("CancellationException" ,ex);
		} catch (ExecutionException ex) {
			logger.error("ExecutionException" ,ex);
		} catch (Exception ex) {
			logger.error("Exception" ,ex);
		} finally {
		}
		Instant stopProducer = Instant.now();
		logger.debug("Time taken to send a message using producer in ms {} ",Duration.between(startProducer, stopProducer).toMillis());
        
		return ResponseEntity.ok().build();
	}

        @ApiOperation(value = "Post records to a topic related to groupId using kafka rest proxy interface ", response = ResponseEntity.class)
	@PostMapping(value = "/restproxy/producers/{topicName}")
	@ResponseBody
	public ResponseEntity<String> produceUsingKafkaRestProxy(@PathVariable String topicName,
			@RequestBody String requestBody) throws JsonProcessingException {

		HttpHeaders headers = new HttpHeaders();
		headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
		HttpEntity<String> entity = new HttpEntity<String>(requestBody, headers);
		Instant startProducer = Instant.now();
		Object obj = null;
		try {
			// For testing purpose emulating the restproxy kafka instance using mocks
			// Run the post response restproxy mock server using npm mock-json-server
			// npx mock-json-server data.json
			// data.json input :
			// {"offsets":[{"partition":0,"offset":3,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}
			logger.debug("Sending a message to a topic  : {}  with content {} ", topicName, requestBody);

			//replace the mock with actual restproxy url
			obj = restTemplate.exchange("http://127.0.0.1:8000/topics/testme", HttpMethod.POST, entity, String.class)
					.getBody();
			logger.debug("Received response  : {} ", obj.toString());

		} catch (Exception ex) {
			logger.error("Exception :", ex);
		} finally {
		}
		Instant stopProducer = Instant.now();
		logger.debug("Time taken to send a message using producer in ms {} ",
				Duration.between(startProducer, stopProducer).toMillis());

		return ResponseEntity.ok(obj.toString());
	}

}
