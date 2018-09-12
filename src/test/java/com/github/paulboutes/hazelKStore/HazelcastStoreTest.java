package com.github.paulboutes.hazelKStore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class HazelcastStoreTest {

  @Test
  void shouldCreateHazelcastStoreBuilderInstance() {
    HazelcastKeyValueStoreFactory<String, Integer> storeBuilder = HazelcastStore
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
