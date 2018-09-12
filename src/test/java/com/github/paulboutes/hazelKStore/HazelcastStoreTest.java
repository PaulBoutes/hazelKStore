package com.github.paulboutes.hazelKStore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class HazelcastStoreTest {

  @Test
  void shouldCreateHazelcastStoreBuilderInstance() {
    HazelcastKeyValueStoreBuilder<String, Integer> storeBuilder = HazelcastStore
        .of("test-store");

    assertThat(storeBuilder).isNotNull();
    assertThat(storeBuilder.name()).isEqualTo("test-store");
  }

  @Test
  void test() throws InterruptedException {


//    Thread.sleep(10000);

//    streams.close();

  }
}
