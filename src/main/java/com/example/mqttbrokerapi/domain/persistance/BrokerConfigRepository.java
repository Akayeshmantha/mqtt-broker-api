package com.example.mqttbrokerapi.domain.persistance;

import reactor.core.publisher.Mono;

public interface BrokerConfigRepository {
    Mono<Void> persistBrokerConfiguration(final String brokerName, final BrokerConfiguration brokerConfiguration);

    Mono<BrokerConfiguration> lookupBrokerConfiguration(final String brokerName);

    Mono<Void> deleteBrokerConfiguration(final String brokerName);
}
