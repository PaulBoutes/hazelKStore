package com.github.paulboutes.hazelKStore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class HazelcastKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final byte[] keyPrefix;
  private final String name;
  private Supplier<HazelcastInstance> hazelcastInstance;
  private boolean open;

  private static <T> Supplier<T> memoize(Supplier<T> provider) {
    ConcurrentHashMap<Object, T> map = new ConcurrentHashMap<>();
    return () -> map.computeIfAbsent("instance", k -> provider.get());
  }

  HazelcastKeyValueStore(Serde<K> keySerde,
                         Serde<V> valueSerde,
                         byte[] keyPrefix,
                         String name, HazelcastProvider hazelcastProvider) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.keyPrefix = keyPrefix;
    this.name = name;
    this.hazelcastInstance = memoize(hazelcastProvider::create);
  }

  @Override public void put(K key, V value) {
    hazelcastInstance.get()
        .getMap("test")
        .put(
            keySerde.serializer().serialize("foo", key),
            valueSerde.serializer().serialize("foo", value)
        );
  }

  @Override public V putIfAbsent(K key, V value) {
    hazelcastInstance.get()
        .getMap("test")
        .putIfAbsent(
            keySerde.serializer().serialize("foo", key),
            valueSerde.serializer().serialize("foo", value)
        );
    return value;
  }

  @Override public void putAll(List<KeyValue<K, V>> entries) {

  }

  @Override public V delete(K key) {
    Object remove = hazelcastInstance.get()
        .getMap("test")
        .remove(keySerde.serializer().serialize("foo", key));
    return valueSerde
        .deserializer()
        .deserialize("foo", (byte[]) remove);
  }

  @Override public String name() {
    return name;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    this.open = true;
    HazelcastInstance instance = this.hazelcastInstance.get();
    if (instance != null && instance.getLifecycleService().isRunning()) {
      if (root != null) {
        context.register(root, false, (key, value) -> {
        });
      }
    }
  }

  @Override public void flush() {
    hazelcastInstance.get().getMap("test").flush();
  }

  @Override public void close() {
    this.open = false;
  }

  @Override public boolean persistent() {
    return true;
  }

  @Override public boolean isOpen() {
    return open;
  }

  @Override public V get(K key) {
    IMap<Object, byte[]> map = hazelcastInstance.get().getMap("test");
    byte[] data = map.get(keySerde.serializer().serialize("foo", key));
    return valueSerde.deserializer().deserialize("foo", data);
  }

  @Override public KeyValueIterator<K, V> range(K from, K to) {
    System.out.println("Create key value iterator range");
    return new HazelcastKeyValueIterator<>();
  }

  @Override public KeyValueIterator<K, V> all() {
    System.out.println("Create key value iterator");
    return new HazelcastKeyValueIterator<>();
  }

  @Override public long approximateNumEntries() {
    return hazelcastInstance.get().getMap("test").size();
  }
}
