package org.example.wikimedia;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Properties;

public class WikiMediaApiKafkaProducer {

    public static void main(String[] args) throws ParseException {

        // Kafka configuration
        String bootstrapServers = "localhost:9092";
        String topic = "wikimedia";

        // Wikimedia API URL
        String wikimediaApiUrl = "https://stream.wikimedia.org/v2/stream/recentchange";


        // Producer configuration
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            // Make a GET request to Wikimedia API


                HttpClient httpClient = HttpClients.createDefault();
                HttpGet request = new HttpGet(wikimediaApiUrl);
                // Send the message to Kafka for each line from the API response
                try {
                    HttpResponse response = httpClient.execute(request);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                    String line;
                    while ((line = reader.readLine()) != null) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
               //         System.out.println(line);
                        // Send the message to Kafka
                        producer.send(record, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception == null) {
                                    System.out.println("Message sent successfully to topic " + metadata.topic());
                                    System.out.println("Partition: " + metadata.partition());
                                    System.out.println("Offset: " + metadata.offset());
                                } else {
                                    System.err.println("Error sending message: " + exception.getMessage());
                                }
                            }
                        });
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
