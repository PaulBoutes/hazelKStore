package com.github.paulboutes.hazelKStore;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HazelcastKeyValueStoreBuilder<K, V> implements StoreBuilder<KeyValueStore<K, V>> {

  private final String name;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;
  private byte[] keyPrefix;
  private byte[] storeKey;
  private boolean cached;
  private HazelcastProvider hazelcastProvider;

  public HazelcastKeyValueStoreBuilder(String name) {
    this.name = name;
  }

  public HazelcastKeyValueStoreBuilder<K, V> withKeySerde(Serde<K> keySerde) {
    this.keySerde = keySerde;
    return this;
  }

  public HazelcastKeyValueStoreBuilder<K, V> withValueSerde(Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return this;
  }

  public HazelcastKeyValueStoreBuilder<K, V> withProvider(HazelcastProvider provider) {
    this.hazelcastProvider = provider;
    return this;
  }

  public HazelcastKeyValueStoreBuilder<K, V> withKeyPrefix(String prefix) {
    this.keyPrefix = prefix.getBytes();
    return this;
  }

  public HazelcastKeyValueStoreBuilder<K, V> withStoreKey(String storeKey) {
    this.storeKey = storeKey.getBytes();
    return this;
  }

  @Override
  public HazelcastKeyValueStoreBuilder<K, V> withCachingEnabled() {
    cached = true;
    return this;
  }

  @Override
  public HazelcastKeyValueStoreBuilder<K, V> withLoggingEnabled(Map<String, String> config) {
    return this;
  }

  @Override public HazelcastKeyValueStoreBuilder<K, V> withLoggingDisabled() {
    return this;
  }

  @Override public KeyValueStore<K, V> build() {
    requireNonNull(keySerde, "keySerde cannot be null");
    requireNonNull(valueSerde, "valueSerde cannot be null");
    requireNonNull(keyPrefix, "keyPrefix cannot be null");
    requireNonNull(name, "name cannot be null");
    requireNonNull(hazelcastProvider, "provider cannot be null");

    return new MeteredKeyValueStore<>(
        new HazelcastKeyValueStore<>(keySerde, valueSerde, keyPrefix, name, hazelcastProvider),
        "hazelcast-stats",
        Time.SYSTEM
    );
  }

  @Override public Map<String, String> logConfig() {
    return Collections.emptyMap();
  }

  @Override public boolean loggingEnabled() {
    return false;
  }

  @Override public String name() {
    return name;
  }
}
