package com.github.paulboutes.hazelKStore;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class HazelcastKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {

  private final Iterator<Entry<byte[], byte[]>> iterator;
  private final AtomicBoolean closed = new AtomicBoolean();
  private boolean isDone = false;

  public HazelcastKeyValueIterator(Iterator<Entry<byte[], byte[]>> iterator) {
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
