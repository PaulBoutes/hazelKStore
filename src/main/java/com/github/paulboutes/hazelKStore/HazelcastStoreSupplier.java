package com.github.paulboutes.hazelKStore;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class HazelcastStoreSupplier implements KeyValueBytesStoreSupplier {

  private final String name;
  private final HazelcastProvider provider;

  public HazelcastStoreSupplier(String name, HazelcastProvider provider) {
    this.name = name;
    this.provider = provider;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    return new HazelcastKeyValueStore(name, provider);
  }

  @Override
  public String metricsScope() {
    return "hazelcast-state";
  }
}
