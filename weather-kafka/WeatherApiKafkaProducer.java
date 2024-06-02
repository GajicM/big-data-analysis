package org.example.weather;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WeatherApiKafkaProducer {

    public static void main(String[] args) throws ParseException {

        // Kafka configuration
        String bootstrapServers = "localhost:9092";
        String topic = "topic-weather-final";

        // Wikimedia API URL
        String wikimediaApiUrl = "http://api.weatherapi.com/v1/history.json";
          String startDate="2023-1-10";
        String query="?q=Belgrade&key=76c9510c7e4d4c2ba4e233724240801&dt=";
   /*     for(int i=0;i<100;i++){
            String date=getDateToUTC(startDate);
            String url=wikimediaApiUrl+query+date;
            System.out.println(url);
            startDate=add10DaysToDateString(startDate);
        }
     */


        // Producer configuration
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            // Make a GET request to Wikimedia API

            for(int i=0;i<=365;i++) {
                Thread.sleep(100);

                HttpClient httpClient = HttpClients.createDefault();
                HttpGet request = new HttpGet(wikimediaApiUrl + query + startDate);
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

                startDate=add1DayToDateString(startDate);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    static String getDateToUTC(String date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date inputDate = sdf.parse(date);

        SimpleDateFormat sdfUtc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdfUtc.setTimeZone(TimeZone.getTimeZone("UTC"));

        return sdfUtc.format(inputDate);
    }
    static String add1DayToDateString(String date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date input= sdf.parse(date);
        input.setTime(input.getTime() + 24 * 60 * 60 * 1000);
        System.out.println(input);
        return sdf.format(input);
    }
}
