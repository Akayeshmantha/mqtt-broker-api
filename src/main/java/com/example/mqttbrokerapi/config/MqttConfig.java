package com.example.mqttbrokerapi.config;

import java.util.function.Function;

import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class MqttConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttConfig.class);

    public static final String DISCONNECTED_FROM_THE_BROKER = "Disconnected from the broker.";

    public static final String TRYING_TO_RECONNECT_TO_THE_BROKER = "Trying to reconnect to the broker";

    @Bean
    public Function<BrokerConfiguration, Mqtt5ReactorClient> Mqtt5ClientFactory() {
        return this::mqtt5Client;
    }

    @Bean
    public Function<String, Mqtt5Subscribe> mqtt5SubscribeFactory() {
        return this::mqtt5Subscribe;
    }

    @Bean
    @Scope(value = "prototype")
    public Mqtt5Subscribe mqtt5Subscribe(final String topic) {
        return Mqtt5Subscribe.builder()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .build();
    }
    @Bean
    @Scope(value = "prototype")
    public Mqtt5ReactorClient mqtt5Client(final BrokerConfiguration brokerConfiguration) {
        Mqtt5Client mqtt5ClientBuilder =  MqttClient.builder()
                .useMqttVersion5()
                .serverHost(brokerConfiguration.getHostName())
                .serverPort(brokerConfiguration.getPort())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(context -> LOGGER.info("Connected to HiveMQ cloud broker"))
                .addDisconnectedListener(context -> {
                    if(context.getReconnector().isReconnect()) {
                        LOGGER.warn(TRYING_TO_RECONNECT_TO_THE_BROKER);
                    } else {
                        LOGGER.info(DISCONNECTED_FROM_THE_BROKER);
                    }
                })
                .sslWithDefaultConfig()
                .simpleAuth()
                .password("f3X!t9QvkPMj.Di".getBytes())
                .username("akayeshmantha")
                .applySimpleAuth()
                .buildRx();

        return Mqtt5ReactorClient.from(mqtt5ClientBuilder);
    }
}
