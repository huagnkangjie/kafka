package com.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @描述：kafka生产者
 *
 * @author 作者 : huang_kangjie
 * @date 创建时间：2017年12月29日
 * @version v1.0.
 * 
 */
public class KafkaProducerTest {

	public static void main(String[] args) {
		produce();
	}
	
	
	@SuppressWarnings("unchecked")
	public static void produce(){
		try {
			Properties props = new Properties();
			 props.put("bootstrap.servers", "172.19.10.42:9092");
			 props.put("acks", "all");
			 props.put("retries", 0);
			 props.put("batch.size", 16384);
			 props.put("linger.ms", 1);
			 props.put("buffer.memory", 33554432);
			 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			@SuppressWarnings("rawtypes")
			Producer<String, String> producer =  new KafkaProducer(props);
			 for(int i = 0; i < 100; i++){
				 
				 Future f = producer.send(new ProducerRecord<String, String>("my-replicated-topic", Integer.toString(i), Integer.toString(i)));
				 System.out.println(f);
			 }
				
			 producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
