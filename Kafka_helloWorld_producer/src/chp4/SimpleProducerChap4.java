package chp4;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerChap4 {
	private static KafkaProducer<String, String> producer;

	public SimpleProducerChap4() {
		Properties props = new Properties();
		
		props.put("bootstrap.servers", "localhost:9092");

		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list", "localhost:9092, localhost:9093, localhost:9094");

		// This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");
		
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//ProducerConfig config = new ProducerConfig(props);
		producer = new KafkaProducer<String, String>(props);
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

		SimpleProducerChap4 simpleProducer = new SimpleProducerChap4();
		simpleProducer.publishMessage(topic, messageCount);
	}

	private void publishMessage(String topic, int messageCount) {
		for (int mCount = 0; mCount < messageCount; mCount++) {
			String runtime = new Date().toString();

			String msg = "Message Publishing Time - " + runtime;
			System.out.println(msg);
			// Creates a KeyedMessage instance
			//KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);

			 producer.send(new ProducerRecord<String, String>(topic, msg));
	            
			
			// Publish the message
			//producer.send(data);
		}
		// Close producer connection with broker.
		producer.close();
	}
}
