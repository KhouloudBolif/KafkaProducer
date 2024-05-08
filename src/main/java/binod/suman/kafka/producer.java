package binod.suman.kafka;



        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.clients.producer.ProducerConfig;
        import org.apache.kafka.clients.producer.ProducerRecord;
        import org.apache.kafka.common.serialization.StringSerializer;

        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.io.IOException;
        import java.util.Properties;
        import java.util.concurrent.Executors;
        import java.util.concurrent.TimeUnit;

public class producer {
    public static int count = 0;



    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        BufferedReader[] br = new BufferedReader[1];

        try {
            br[0] = new BufferedReader(new FileReader("C:\\Users\\khoul\\Dropbox\\PC\\Downloads\\kafka-spark-streaming-integration-master\\kafka-spark-streaming-integration-master\\kafka\\src\\main\\resources\\churn-bigml-80.csv"));
            br[0].readLine(); // Ignorer l'en-tête du fichier CSV

            // Scheduled task to continuously send messages
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
                String line;
                try {
                    if ((line = br[0].readLine()) != null) {
                        String key = String.valueOf(++count);
                        // Envoyer la ligne au topic Kafka
                        kafkaProducer.send(new ProducerRecord<>("test1", key, line));
                        System.out.println("key=>" + key + " messages=>" + line);
                    } else {
                        // Si le fichier est terminé, fermez le flux de lecture et le producteur
                        br[0].close();
                        kafkaProducer.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, 0, 2, TimeUnit.SECONDS);

        } catch (IOException e) {
            e.printStackTrace();
            // Si une exception se produit lors de l'ouverture du fichier, fermez le flux de lecture
            if (br[0] != null) {
                try {
                    br[0].close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }
    }
}