package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> wordCountInput = builder.stream("word-count-input"); // 1 - stream from Kafka

        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(String::toLowerCase) // 2 - map values to lowercase
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split("\\W+"))) // 3 - flatMap values split by space
                .selectKey((ignoredKey, word) -> word) // 4 - select key to apply a key (we discard the old key)
                .groupByKey() // 5 - group by key before aggregation
                .count("Counts"); // 6 - count occurences

        wordCounts.to(Serdes.String(),Serdes.Long(),"word-count-output"); // 7 - to in order to write the results back to kafka

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
