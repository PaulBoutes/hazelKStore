package com.github.paulboutes.hazelKStore.state;

import com.github.paulboutes.hazelKStore.hazelcast.HazelcastProvider;
import com.github.paulboutes.hazelKStore.state.internals.HazelcastStoreSupplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class HazelcastStore {

  public static HazelcastStoreSupplier storeSupplier(String name, HazelcastProvider provider) {
    return new HazelcastStoreSupplier(name, provider);
  }

  public static <K, V> Builder<K, V> builder(String name) {
    return new Builder<>(name);
  }

  public static class Builder<K, V> {

    private String name;
    private HazelcastProvider provider;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    private Builder(String name) {
      this.name = name;
    }

    public Builder<K, V> keySerde(Serde<K> keySerde) {
      this.keySerde = keySerde;
      return this;
    }

    public Builder<K, V> valueSerde(Serde<V> valueSerde) {
      this.valueSerde = valueSerde;
      return this;
    }

    public Builder<K, V> provider(HazelcastProvider provider) {
      this.provider = provider;
      return this;
    }

    public StoreBuilder<KeyValueStore<K, V>> storeBuilder() {
      return Stores.keyValueStoreBuilder(storeSupplier(name, provider), keySerde, valueSerde);
    }

  }

}
