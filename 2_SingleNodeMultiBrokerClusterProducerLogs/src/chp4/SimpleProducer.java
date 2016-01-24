package chp4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class SimpleProducer {
	private static KafkaProducer<Object, String> producer;

	public SimpleProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092, localhost:9093, localhost:9094");
		//props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "chp4.MyPartitioner");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// ProducerConfig config = new ProducerConfig(props);
		producer = new KafkaProducer<Object, String>(props);
	}

	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException("Please provide topic name and Message count as arguments");

		// Topic name and the message count to be published is passed from the
		// command line
		String topic = (String) args[0];
		String count = (String) args[1];
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);

		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}

	private void publishMessage(String topic, int messageCount) {
		Random random = new Random();

		for (int mCount = 0; mCount < messageCount; mCount++) {

			String clientIP = "192.168.14." + random.nextInt(5); 
			String accessTime = new Date().toString();

			String message = accessTime + ",kafka.apache.org," + clientIP;
			System.out.print(message);

			// Creates a KeyedMessage instance
			// KeyedMessage<String, String> data = new KeyedMessage<String,
			// String>(topic, msg);
			
			producer.send(new ProducerRecord<Object, String>(topic, clientIP, message));

			// Publish the message
			// producer.send(data);
		}
		// Close producer connection with broker.
		producer.close();
	}
}
