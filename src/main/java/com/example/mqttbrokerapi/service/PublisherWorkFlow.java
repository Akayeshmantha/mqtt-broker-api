package com.example.mqttbrokerapi.service;

import java.nio.charset.StandardCharsets;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class PublisherWorkFlow {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnectWorkFlow.class);

	public Flux<Mqtt5PublishResult> publishWorkFlow(final Mqtt5ReactorClient mqtt5ReactorClient,
			final String message, final String topicName) {
		return mqtt5ReactorClient
				.publish(pub -> pub.onNext(buildPayload(message, topicName)))
				.doOnError(throwable -> LOGGER.error("Error during publishing ", throwable))
				.doOnNext(pubResult -> LOGGER.info("Published " + new String(pubResult.getPublish().getPayloadAsBytes())));
	}

	public Mqtt5Publish buildPayload(final String payload, final String topicName) {
		return Mqtt5Publish
				.builder()
				.topic(topicName)
				.qos(MqttQos.AT_LEAST_ONCE)
				.payload(payload.getBytes(StandardCharsets.UTF_8))
				.retain(false)
				.build();
	}
}
