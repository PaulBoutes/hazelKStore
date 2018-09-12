package com.github.paulboutes.hazelKStore;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class TestLow {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testapp");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dbts-kafka01.cultura.intra:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    final StoreBuilder<KeyValueStore<String, Integer>> test = Stores.keyValueStoreBuilder(
        new HazelcastStoreSupplier("testlow", HazelcastProvider.defaultClient()),
        Serdes.String(),
        Serdes.Integer()
    );

    final Topology topology = new Topology();

    topology
        .addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), "foo")
        .addProcessor("processor", () -> new ProcessorNode(test.name()), "source")
        .addStateStore(test, "processor");

    final KafkaStreams kafkaStreams = new KafkaStreams(topology, new StreamsConfig(props));

    kafkaStreams.cleanUp();
    kafkaStreams.start();

  }

  public static class ProcessorNode implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore<String, Integer> kvStore;
    private String name;

    public ProcessorNode(String name) {
      this.name = name;
    }

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
      this.kvStore = (KeyValueStore) context.getStateStore(name);
    }

    @Override
    public void process(String key, String value) {
      System.out.println("Key = " + key);
      final Integer count = this.kvStore.get(key);
      System.out.println("Count = " + count);
      if (count != null) {
        this.kvStore.put(key, count + 1);
      } else {
        this.kvStore.put(key, 1);
      }
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
  }

}
