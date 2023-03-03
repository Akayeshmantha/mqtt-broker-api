package com.example.mqttbrokerapi.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.example.mqttbrokerapi.controller.v1.MqttOperationController;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfigRepository;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import com.example.mqttbrokerapi.service.ClientConnectWorkFlow;
import com.example.mqttbrokerapi.service.PublisherWorkFlow;
import com.example.mqttbrokerapi.service.SubscriptionWorkFlow;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
@ExtendWith(SpringExtension.class)
@WebFluxTest(MqttOperationController.class)
@ContextConfiguration(classes = {PublisherWorkFlow.class,SubscriptionWorkFlow.class, MqttOperationController.class,
		BrokerConfigRepository.class, ClientConnectWorkFlow.class})
public class MqttOperationControllerIntegration {

	@Autowired
	private PublisherWorkFlow publisherWorkFlow;

	@Autowired
	private SubscriptionWorkFlow subscriptionWorkFlow;

	@Container
	@NotNull HiveMQContainer container = new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce:latest"));

	private @NotNull Mqtt5BlockingClient mqtt5BlockingClient;

	private @NotNull Mqtt5ReactorClient mqtt5ReactorClient;
	@MockBean
	private BrokerConfigRepository brokerConfigRepository;
	@MockBean
	private Function<BrokerConfiguration, Mqtt5ReactorClient> mqtt5ClientFactory;

	@MockBean
	Function<String, Mqtt5Subscribe> mqtt5SubscribeFactory;

	@Autowired
	private ClientConnectWorkFlow clientConnectWorkFlow;

	private BrokerConfiguration brokerConfiguration;

	private final static String BROKER_NAME = "broker1";

	public static final String MESSAGE_TO_THE_BORKER = "Message to the borker";

	public static final String TOPIC_1 = "topic1";

	private Mqtt5Subscribe mqtt5Subscribe;

	@Autowired
	private WebTestClient webClient;
	@BeforeEach
	public void setUp() {
		mqtt5BlockingClient = MqttClient.builder()
				.serverPort(container.getMqttPort()).useMqttVersion5().buildBlocking();
		brokerConfiguration =  BrokerConfiguration.builder()
				.port(container.getMqttPort())
				.hostName(container.getHost())
				.build();

		mqtt5Subscribe =  Mqtt5Subscribe.builder()
				.topicFilter(TOPIC_1)
				.qos(MqttQos.AT_LEAST_ONCE)
				.build();
		mqtt5ReactorClient = Mqtt5ReactorClient.from(MqttClient.builder()
				.serverPort(container.getMqttPort()).useMqttVersion5().buildRx());

	}

	@Test
	public void testPublishWorkFlow() throws InterruptedException {

		var subscriber = mqtt5BlockingClient.publishes(MqttGlobalPublishFilter.ALL);
		mqtt5BlockingClient.connect();
		mqtt5BlockingClient.subscribeWith().topicFilter(TOPIC_1).send();

		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));
		when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(mqtt5ReactorClient);
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(MESSAGE_TO_THE_BORKER))
				.exchange()
				.expectStatus()
				.isOk();

		var receive = subscriber.receive();
		assertEquals( MESSAGE_TO_THE_BORKER, new String(receive.getPayloadAsBytes()));
	}

	@Test
	public void testSubscribeWorkFlow() throws InterruptedException, ExecutionException {
		var publishes = mqtt5BlockingClient.publishes(MqttGlobalPublishFilter.ALL);
		mqtt5BlockingClient.connect();
		Mqtt5Publish mqtt5Publish = Mqtt5Publish
				.builder()
				.topic(TOPIC_1)
				.qos(MqttQos.AT_LEAST_ONCE)
				.payload(MESSAGE_TO_THE_BORKER.getBytes(StandardCharsets.UTF_8))
				.retain(false)
				.build();

		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(mqtt5ReactorClient);
		when(mqtt5SubscribeFactory.apply(TOPIC_1)).thenReturn(mqtt5Subscribe);

		CompletableFuture<Void> completableFuture = runInFuture();
		Thread.sleep(150);
		mqtt5BlockingClient.publish(mqtt5Publish);
		completableFuture.get();
	}


	private CompletableFuture<Void> runInFuture() {
		CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
			Flux<byte[]> flux = webClient
					.get().uri("/mqtt/"+ BROKER_NAME +"/get/"+ TOPIC_1)
					.accept(MediaType.valueOf("application/stream+json"))
					.exchange()
					.expectStatus()
					.isOk()
					.returnResult(byte[].class)
					.getResponseBody();

			StepVerifier.create(flux)
					.expectSubscription()
					.assertNext(bytes -> assertEquals(MESSAGE_TO_THE_BORKER, new String(bytes)))
					.thenCancel()
					.verify();
		});
		return completableFuture;
	}
}
