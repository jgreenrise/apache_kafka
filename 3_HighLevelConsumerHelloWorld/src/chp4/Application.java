package chp4;

public class Application {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SimpleProducer producerThread = new SimpleProducer(KafkaProperties.TOPIC, 20);
		producerThread.start();

        HighLevelConsumer consumerThread = new HighLevelConsumer(KafkaProperties.TOPIC);
        consumerThread.start();

	}

}
