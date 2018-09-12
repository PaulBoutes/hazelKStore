package com.github.paulboutes.hazelKStore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class Test {

  private static final byte[] toByteArray(int value) {
    return new byte[] {
        (byte) (value >>> 24),
        (byte) (value >>> 16),
        (byte) (value >>> 8),
        (byte) value};
  }

  public static int fromByteArray(byte[] bytes) {
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testapp");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    //    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder
        .stream("foo", Consumed.with(Serdes.String(), Serdes.String()));

    stream.print(Printed.toSysOut());

    stream
        .groupByKey()
        .aggregate(
            () -> 0,
            (key, value, aggregate) -> aggregate + 1,
            Materialized.as(new HazelcastStoreSupplier("toto"))
        )
        .toStream()
        .foreach((k, v) -> System.out.println("(" + k + " -> " + v + ")"));

    KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(props));

    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
