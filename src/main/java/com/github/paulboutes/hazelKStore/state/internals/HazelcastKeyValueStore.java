package com.github.paulboutes.hazelKStore.state.internals;

import static java.util.stream.Collectors.toMap;

import com.github.paulboutes.hazelKStore.hazelcast.HazelcastProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

class HazelcastKeyValueStore implements KeyValueStore<Bytes, byte[]> {

  private final String name;
  private ProcessorContext context;
  private Supplier<HazelcastInstance> hazelcastInstance;
  private boolean open;
  private IMap<byte[], byte[]> map;

  private static Supplier<HazelcastInstance> memoize(Supplier<HazelcastInstance> provider, String name) {
    ConcurrentHashMap<String, HazelcastInstance> map = new ConcurrentHashMap<>();
    return () -> map.computeIfAbsent(name, k -> provider.get());
  }

  HazelcastKeyValueStore(String name, HazelcastProvider hazelcastProvider) {
    this.name = name;
    this.hazelcastInstance = memoize(hazelcastProvider::create, name);
  }


  private static class HazelcastBatchingRestoreCallback extends
      AbstractNotifyingBatchingRestoreCallback {

    private final HazelcastKeyValueStore hazelcastKeyValueStore;

    HazelcastBatchingRestoreCallback(final HazelcastKeyValueStore hazelcastKeyValueStore) {
      this.hazelcastKeyValueStore = hazelcastKeyValueStore;
    }

    @Override
    public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
      records.forEach(kv -> {
        if (kv.value == null) {
          hazelcastKeyValueStore.map.removeAsync(kv.key);
        } else {
          hazelcastKeyValueStore.map.putAsync(kv.key, kv.value);
        }
      });
    }

    @Override
    public void onRestoreStart(final TopicPartition topicPartition, final String storeName,
        final long startingOffset,
        final long endingOffset) {
      // do nothing
    }

    @Override
    public void onRestoreEnd(final TopicPartition topicPartition, final String storeName,
        final long totalRestored) {
      // do nothing
    }
  }


  @Override
  public String name() {
    return name;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    this.open = true;
    HazelcastInstance instance = this.hazelcastInstance.get();
    if (instance != null && instance.getLifecycleService().isRunning() && root != null) {
      context.register(root, false, new HazelcastBatchingRestoreCallback(this));
      this.context = context;
      map = hazelcastInstance.get().getMap(name);
    }
  }

  @Override
  public void flush() {

  }

  @Override
  public void close() {
    this.open = false;
  }

  @Override
  public boolean persistent() {
    return true;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void put(Bytes key, byte[] value) {
    map.put(key.get(), value);
  }

  @Override
  public byte[] putIfAbsent(Bytes key, byte[] value) {
    return map.putIfAbsent(key.get(), value);
  }

  @Override
  public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
    map.putAll(entries.stream().collect(toMap(e -> e.key.get(), e -> e.value)));
  }

  @Override
  public byte[] delete(Bytes key) {
    return map.remove(key.get());
  }

  @Override
  public byte[] get(Bytes key) {
    return map.get(key.get());
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    return new HazelcastKeyValueIterator(map.entrySet(rangePredicate(from, to)).iterator());
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return new HazelcastKeyValueIterator(map.entrySet().iterator());
  }

  @Override
  public long approximateNumEntries() {
    return map.size();
  }

  private Predicate<byte[], byte[]> rangePredicate(Bytes from, Bytes to) {
    return e -> {
      final Bytes bytes = Bytes.wrap(e.getKey());
      return from.compareTo(bytes) <= 0 && bytes.compareTo(to) <= 0;
    };
  }


  private class HazelcastKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {

    private final Iterator<Entry<byte[], byte[]>> iterator;
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean isDone = false;

    HazelcastKeyValueIterator(Iterator<Entry<byte[], byte[]>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void close() {
      closed.set(true);
      isDone = true;
    }

    @Override
    public Bytes peekNextKey() {
      throw new UnsupportedOperationException("peekNextKey() is not supported");
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more element");
      }
      final Entry<byte[], byte[]> next = iterator.next();
      final Bytes parsedKey = Bytes.wrap(next.getKey());
      return KeyValue.pair(parsedKey, next.getValue());
    }
  }


}
