import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerApplication {

  private static final String TOPIC_NAME = "topic-jaeshim";
  private static final String GROUP_ID = "jaeshim-group";

  private static final String BOOTSTRAP_SERVERS="localhost:9092,localhost:9093,localhost:9094,localhost:9095";



  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerApplication.class);
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5); //max.poll.records
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); //auto commit interval

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Collections.singletonList(TOPIC_NAME));

    String message = null;

    try {
      int ct = 1;
      do {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000L));
        ct = 1;
        logger.info("message consumed " + records.count());

        for (ConsumerRecord<String, String> record : records) {

          logger.info(
              "offset: " + record.offset() + ", count: " + (ct++) + " of " + records.count());
        }

      } while (true);
    } catch (Exception e) {

    } finally {
      consumer.close();
    }
  }
}
