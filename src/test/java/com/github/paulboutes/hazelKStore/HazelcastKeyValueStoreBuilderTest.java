package com.github.paulboutes.hazelKStore;


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NetworkConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class HazelcastKeyValueStoreBuilderTest {

  @Nested
  class StoreBuilderWithMissingParam {

    @Test
    void shouldFailedWithMissingParams() {
      HazelcastKeyValueStoreBuilder<String, Integer> storeBuilder =
          new HazelcastKeyValueStoreBuilder<>("test");
      assertThatThrownBy(storeBuilder::build)
          .isInstanceOf(NullPointerException.class)
          .hasMessage("keySerde cannot be null");
    }

    @Test
    void shouldFailedWithMissingValueSerde() {
      HazelcastKeyValueStoreBuilder<String, Integer> storeBuilder =
          new HazelcastKeyValueStoreBuilder<>("test");
      assertThatThrownBy(() -> storeBuilder.withKeySerde(Serdes.String()).build())
          .isInstanceOf(NullPointerException.class)
          .hasMessage("valueSerde cannot be null");
    }

  }

  @Nested
  class StoreBuilderSuccess {

    @Test
    void shouldSucceed() {

      ClientConfig clientConfig = new ClientConfig();
      clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");

      HazelcastKeyValueStore<String, Integer> store = new HazelcastKeyValueStore<>(
          Serdes.String(),
          Serdes.Integer(),
          "keyp".getBytes(),
          "test",
          HazelcastProvider.of(clientConfig)
      );

      ProcessorContext context = mock(ProcessorContext.class);
      doReturn("app").when(context).applicationId();
      doReturn("topic").when(context).topic();
      doReturn(0).when(context).partition();
      doReturn(new TaskId(0, 0)).when(context).taskId();

      store.init(context, null);



    }
  }


}
