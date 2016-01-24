package chp4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class SimpleProducer extends Thread {
	private static KafkaProducer<Object, String> producer;
	private final String topic;
	private final int messageCount;

	public SimpleProducer(String topic, int messageCount) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092, localhost:9093, localhost:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// ProducerConfig config = new ProducerConfig(props);
		producer = new KafkaProducer<Object, String>(props);
		this.topic = topic;
		this.messageCount = messageCount;
	}

	public void run() {
		Random random = new Random();

		for (int mCount = 0; mCount < messageCount; mCount++) {

			String clientIP = "192.168.14." + random.nextInt(5); 
			String accessTime = new Date().toString();

			String message = accessTime + ",kafka.apache.org," + clientIP;
			System.out.println("Producer "+topic+"\t Message: "+message);
			
			producer.send(new ProducerRecord<Object, String>(topic, clientIP, message));
		}
		// Close producer connection with broker.
		producer.close();
	}
}
