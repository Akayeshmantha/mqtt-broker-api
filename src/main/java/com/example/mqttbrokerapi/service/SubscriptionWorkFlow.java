package com.example.mqttbrokerapi.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
public class SubscriptionWorkFlow {
	private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionWorkFlow.class);
	Function<String, Mqtt5Subscribe> mqtt5SubscribeFactory;

	public SubscriptionWorkFlow(Function<String, Mqtt5Subscribe> mqtt5SubscribeFactory) {
		this.mqtt5SubscribeFactory = mqtt5SubscribeFactory;
	}

	public Flux<String> subscriptionWorkFlow(Mqtt5ReactorClient mqtt5ReactorClient, String topic) {
		return mqtt5ReactorClient.subscribePublishes(mqtt5SubscribeFactory.apply(topic))
				.doOnNext(this::logMediator)
				.map( mqtt5Publish -> String.format("%s",
						new String(mqtt5Publish.getPayloadAsBytes())))
				.publishOn(Schedulers.parallel());
	}
	private void logMediator(Mqtt5Publish mqtt5Publish) {
		LOGGER.info("Message received from topic {}: {} ", mqtt5Publish.getTopic(),
				StandardCharsets.UTF_8.decode(ByteBuffer.wrap(mqtt5Publish.getPayloadAsBytes())));
	}
}
