package com.example.mqttbrokerapi.domain.persistance.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.example.mqttbrokerapi.domain.persistance.BrokerConfigRepository;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public class BrokerConfigRepositoryImpl implements BrokerConfigRepository {
    private final ConcurrentMap<String, BrokerConfiguration> configStore = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> persistBrokerConfiguration(final String brokerName, final BrokerConfiguration brokerConfiguration) {
        if (Strings.isEmpty(brokerName)){
            return Mono.empty();
        }

        configStore.put(brokerName, brokerConfiguration);
        return Mono.empty();
    }

    @Override
    public Mono<BrokerConfiguration> lookupBrokerConfiguration(final String brokerName) {
        return Mono.justOrEmpty(configStore.get(brokerName));
    }

    @Override
    public Mono<Void> deleteBrokerConfiguration(final String brokerName) {
        configStore.remove(brokerName);
        return Mono.empty();
    }
}
