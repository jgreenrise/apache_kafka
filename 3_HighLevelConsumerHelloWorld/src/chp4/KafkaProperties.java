package chp4;

public interface KafkaProperties {
    String ZK_CONNECT = "localhost:2181";
    String GROUP_ID = "testgroup";
    String TOPIC = "topic1";
    String KAFKA_SERVER_URL = "localhost";
    int KAFKA_SERVER_PORT = 9092;
    int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    int CONNECTION_TIMEOUT = 100000;
    int RECONNECT_INTERVAL = 10000;
    String TOPIC2 = "topic2";
    String TOPIC3 = "topic3";
    String CLIENT_ID = "SimpleConsumerDemoClient";
}
