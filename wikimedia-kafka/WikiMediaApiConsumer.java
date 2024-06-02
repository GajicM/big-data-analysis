package org.example.wikimedia;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class WikiMediaApiConsumer {
    static AtomicBoolean hasHeader= new AtomicBoolean(true);
    public static void main(String[] args) {
        // Kafka configuration
        String bootstrapServers = "localhost:9092";
        String groupId = "Veliki Podaci Ispit streaming";
        String topic = "wikimedia";

        // Kafka consumer configuration
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the Kafka topic
            consumer.subscribe(Collections.singletonList(topic));

            // Process Kafka records and write to CSV
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {
                    // Write Kafka records to CSV
                    writeToCSV(records);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeToCSV(ConsumerRecords<String, String> records) {
        // Specify the CSV file path
        String csvFilePath = "C:\\Users\\milos\\Desktop\\kafka_wikimedia3.csv";
        File csvFile = new File(csvFilePath);

        // Create CsvMapper and CsvSchema with dynamic column names
        CsvMapper csvMapper = new CsvMapper();

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile,true))) {
            // Access the first record to determine the header



            // Iterate through Kafka records and write to CSV
            records.forEach(record -> {
             //   System.out.println(record.value());
               String[] data=record.value().split("\n ");
               for(String s:data){
                 if(s.startsWith("data:")){
                     s=s.substring(5);

                     try {
                    if (!hasHeader.get()) {
                        ObjectMapper jsonMapper = new ObjectMapper();
                        JsonNode rootNode = jsonMapper.readTree(s);
                        flattenNestedStructures((ObjectNode) rootNode);
                        CsvSchema csvSchema = createDynamicCsvSchema(rootNode).withoutQuoteChar();
                        String[] colNames=csvSchema.getColumnDesc().split(",");
                        for(int i=0;i<colNames.length;i++){
                            colNames[i]=colNames[i].replaceAll("[\"\\]\\[]","");
                        }
                        System.out.println(Arrays.toString(colNames));
                        writer.writeNext(colNames); // Write header only once
                        hasHeader.set(true);
                    }
                    ObjectMapper jsonMapper = new ObjectMapper();
                    JsonNode rootNode = jsonMapper.readTree(s);
                         flattenNestedStructures((ObjectNode) rootNode);
                    CsvSchema csvSchema = createDynamicCsvSchema(rootNode).withoutQuoteChar();

                    String[] csvData = csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(rootNode).replaceAll("[\"'\\]\\[\n]","").split("[,\n]");
                    System.out.println(Arrays.toString(csvData));
                    writer.writeNext(csvData);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                 }
               }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static CsvSchema createDynamicCsvSchema(JsonNode hoursArray) {
        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        Iterator<String> fieldNames = hoursArray.fieldNames();
        System.out.println(fieldNames);
        for (Iterator<String> it = fieldNames; it.hasNext(); ) {
            String fieldName = it.next();
            csvSchemaBuilder.addColumn(fieldName);

        }
        return csvSchemaBuilder.build();
    }

    private static void flattenNestedStructures(ObjectNode hourNode) {
        hourNode.set("length", hourNode.path("length").path("old"));
        hourNode.set("length_new", hourNode.path("length").path("new"));
        hourNode.set("revision", hourNode.path("revision").path("old"));
        hourNode.set("revision_new", hourNode.path("revision").path("new"));

        hourNode.get("meta").fieldNames().forEachRemaining(s -> {
            hourNode.set(s, hourNode.path("meta").path(s));
        });
        hourNode.remove("meta");
        System.out.println(hourNode.toPrettyString());

    }
}


