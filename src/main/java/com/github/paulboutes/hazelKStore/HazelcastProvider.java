package com.github.paulboutes.hazelKStore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

public interface HazelcastProvider {

  HazelcastInstance create();

  static HazelcastProvider of(ClientConfig clientConfig) {
    clientConfig.setProperty("hazelcast.logging.type", "slf4j");
    return () -> HazelcastClient.newHazelcastClient(clientConfig);
  }

  static HazelcastProvider defaultClient() {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.setProperty("hazelcast.logging.type", "slf4j");
    clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
    return () -> HazelcastClient.newHazelcastClient(clientConfig);
  }

}
