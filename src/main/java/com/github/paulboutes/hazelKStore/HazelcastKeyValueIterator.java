package com.github.paulboutes.hazelKStore;

import com.hazelcast.core.IMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;

public class HazelcastKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

  @Override public void close() {
    // do nothing
  }

  @Override public K peekNextKey() {
    throw new UnsupportedOperationException("peekNextKey() is not supported");
  }

  @Override public boolean hasNext() {
    return false;
  }

  @Override public KeyValue<K, V> next() {
    return null;
  }
}
