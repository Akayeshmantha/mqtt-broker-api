package com.example.mqttbrokerapi.service;

import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ClientConnectWorkFlow {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnectWorkFlow.class);

	public Mono<Mqtt5ConnAck> connectSubscriber(Mqtt5ReactorClient mqtt5Client) {

		if (mqtt5Client.getState().isConnectedOrReconnect()) {
			LOGGER.debug("Client is connected");
			return Mono.empty();
		}

		return mqtt5Client
				.connectWith()
				.applyConnect()
				.retry(3);
	}
}
