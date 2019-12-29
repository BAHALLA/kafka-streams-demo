package org.sid;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamConsumer {
    public static void main(String[] args) {
        new KafkaStreamConsumer().start();
    }

    private void start() {
        Properties properties =new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-1");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream("glsidTopic",
                Consumed.with(Serdes.String(), Serdes.String()));
        KTable<Windowed<String>, Long> resultStream =  kStream.flatMapValues(text -> Arrays.asList(text.split("\\W+")))
                .map((k,v) -> new KeyValue<>(k, v.toLowerCase()))
                .filter((k,v) -> v.equals("a") || v.equals("b"))
                .groupBy((k,v) -> v)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count(Materialized.as("count-analytics"));
                /*.foreach( (k,v) ->{
                     System.out.println(" key ="+ k+ " value=" + v);
                 });*/
                resultStream.toStream().map((k,v) -> new KeyValue<>(k.key(), v)).
                        to("resTopic", Produced.with(Serdes.String(), Serdes.Long()));


        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
