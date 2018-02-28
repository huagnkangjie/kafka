package com.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @描述：
 *
 * @author 作者 : huang_kangjie
 * @date 创建时间：2017年12月29日
 * @version v1.0.
 * 
 */
public class KafkaCunsumerTest {

	
	public static void main(String[] args) {
		cunsumer();
	}
	
	public static void cunsumer(){
		try {
			Properties props = new Properties();
		     props.put("bootstrap.servers", "172.19.10.42:9092");
		     props.put("group.id", "test");
		     props.put("enable.auto.commit", "false");
		     props.put("auto.commit.interval.ms", "1000");
		     props.put("session.timeout.ms", "30000");
		     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		     consumer.subscribe(Arrays.asList("my-replicated-topic"));
		     final int minBatchSize = 200;
		     List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		     while (true) {
		         ConsumerRecords<String, String> records = consumer.poll(100);
		         for (ConsumerRecord<String, String> record : records) {
		             buffer.add(record);
		         }
		         if (buffer.size() >= minBatchSize) {
		            System.out.println(buffer);
		             consumer.commitSync();
		             buffer.clear();
		         }
		     }
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
