package binod.suman.kafka;



import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class CSVKafkaProducer {

	private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "test10";
    private static String CsvFile = "churn-bigml-80.csv";
    
  
    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        
        CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
        kafkaProducer.publishMessages();
        System.out.println("Producing job completed");
    }



    private void publishMessages() throws URISyntaxException {
        final Producer<String, String> csvProducer = ProducerProperties();

        try {
            URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> fileStream = Files.lines(Paths.get(uri));

            AtomicInteger counter = new AtomicInteger(0); // Compteur pour suivre le numéro de ligne

            fileStream.forEach(line -> {
                if (counter.getAndIncrement() > 0) { // Skip the first line
                    System.out.println(line);

                    final ProducerRecord<String, String> csvRecord = new ProducerRecord<>(
                            KafkaTopic, UUID.randomUUID().toString(), line);

                    csvProducer.send(csvRecord, (metadata, exception) -> {
                        if (metadata != null) {
                            System.out.println("CsvData: -> " + csvRecord.key() + " | " + csvRecord.value());
                        } else {
                            System.out.println("Error Sending Csv Record -> " + csvRecord.value());
                        }
                    });
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
