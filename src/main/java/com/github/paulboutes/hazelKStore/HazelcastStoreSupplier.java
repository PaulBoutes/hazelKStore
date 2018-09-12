package com.github.paulboutes.hazelKStore;

import com.hazelcast.client.config.ClientConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class HazelcastStoreSupplier implements KeyValueBytesStoreSupplier {

  private final String name;

  public HazelcastStoreSupplier(String name) {
    this.name = name;
  }

  @Override public String name() {
    return name;
  }

  @Override public KeyValueStore<Bytes, byte[]> get() {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
    return new HazelcastKeyValueStore<>(
        Serdes.Bytes(),
        Serdes.ByteArray(),
        "kp".getBytes(),
        name,
        HazelcastProvider.of(clientConfig)
    );
  }

  @Override public String metricsScope() {
    return "hazelcast-state";
  }
}
