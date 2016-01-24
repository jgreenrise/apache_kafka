package chp4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class SimpleProducerChap4 {
	private static KafkaProducer<String, String> producer;

	public SimpleProducerChap4() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092, localhost:9093, localhost:9094");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}

	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException("Please provide topic name and Message count as arguments");
		String topic = (String) args[0];
		String count = (String) args[1];
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);
		SimpleProducerChap4 simpleProducer = new SimpleProducerChap4();
		simpleProducer.publishMessage(topic, messageCount);
	}

	public void publishMessage(String topic, int messageCount) {
		for (int mCount = 0; mCount < messageCount; mCount++) {
			String runtime = new Date().toString();

			String msg = "Message Publishing Time - " + runtime;
			System.out.println(msg);
			producer.send(new ProducerRecord<String, String>(topic, msg));
		}
		producer.close();
	}
}
