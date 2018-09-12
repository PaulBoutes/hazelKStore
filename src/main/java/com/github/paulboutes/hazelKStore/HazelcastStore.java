package com.github.paulboutes.hazelKStore;

public class HazelcastStore {

  private HazelcastStore() {}

  public static <K, V> HazelcastKeyValueStoreBuilder<K, V> of(String name) {
    return new HazelcastKeyValueStoreBuilder<>(name);
  }

}
