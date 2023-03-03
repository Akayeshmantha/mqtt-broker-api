package com.example.mqttbrokerapi.unit;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import com.example.mqttbrokerapi.controller.v1.MqttOperationController;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfigRepository;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import com.example.mqttbrokerapi.dto.CreateBrokerConfigDTO;
import com.example.mqttbrokerapi.exception.BrokerConfigException;
import com.example.mqttbrokerapi.service.ClientConnectWorkFlow;
import com.example.mqttbrokerapi.service.PublisherWorkFlow;
import com.example.mqttbrokerapi.service.SubscriptionWorkFlow;
import com.hivemq.client.internal.mqtt.message.connect.connack.MqttConnAck;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublishResult;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(SpringExtension.class)
@WebFluxTest(MqttOperationController.class)
public class MqttOperationControllerTest {

	public static final String NOT_EXITS = "NOT_EXITS";

	public static final String TOPIC_1 = "topic1";

	public static final String MESSAGE_TO_THE_BORKER = "Message to the borker";

	@Autowired
	private WebTestClient webClient;
	@MockBean
	private BrokerConfigRepository brokerConfigRepository;
	@MockBean
	private Function<BrokerConfiguration, Mqtt5ReactorClient> mqtt5ClientFactory;
	@MockBean
	private SubscriptionWorkFlow subscriptionWorkFlow;
	@MockBean
	private ClientConnectWorkFlow clientConnectWorkFlow;
	@MockBean
	private PublisherWorkFlow publisherWorkFlow;
	@MockBean
	private Mqtt5ReactorClient mqtt5ReactorClient;
	@MockBean
	private MqttConnAck mqttConnAck;
	@MockBean
	private MqttPublishResult mqttPublishResult;


	private final static String BROKER_NAME = "broker1";
	private final static int SSL_PORT = 8883;
	private final static String HOST_NAME = "sample.s2.eu.hivemq.cloud";
	private BrokerConfiguration brokerConfiguration;
	private CreateBrokerConfigDTO createBrokerConfigDTO;
	@BeforeEach
	public void setUp() {
		brokerConfiguration =  BrokerConfiguration.builder()
				.port(SSL_PORT)
				.hostName(HOST_NAME)
				.build();

		createBrokerConfigDTO = CreateBrokerConfigDTO.builder()
				.port(SSL_PORT)
				.hostName(HOST_NAME)
				.build();
	}

	@Test
	public void shouldGetBrokerConfiguration() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));

		webClient
				.get().uri("/mqtt/"+ BROKER_NAME )
				.exchange()
				.expectStatus()
				.isOk()
				.expectBody(BrokerConfiguration.class);
	}
	@Test
	public void getProductListWithNotExisting_ShouldReturnEmptyResponse() {
		when(brokerConfigRepository.lookupBrokerConfiguration(NOT_EXITS)).thenReturn(Mono.empty());
		webClient
				.get().uri("/mqtt/"+ NOT_EXITS )
				.exchange()
				.expectStatus()
				.isOk()
				.expectBody().isEmpty();
	}
	@Test
	public void shouldCreateBrokerConfiguration() {
		when(brokerConfigRepository.persistBrokerConfiguration(eq(BROKER_NAME), any()))
				.thenReturn(Mono.empty());
		webClient
				.put().uri("/mqtt/"+ BROKER_NAME )
				.contentType(MediaType.APPLICATION_JSON)
				.body(BodyInserters.fromObject(createBrokerConfigDTO))
				.exchange()
				.expectStatus()
				.isOk()
				.expectBody().isEmpty();
	}
	@Test
	public void shouldThrowBadRequest_WithEmoteOrHalfBrokerConfiguration() {
		webClient
				.put().uri("/mqtt/"+ BROKER_NAME )
				.contentType(MediaType.APPLICATION_JSON)
				.body(null)
				.exchange()
				.expectStatus()
				.isBadRequest();

		final BrokerConfiguration brokerConfiguration1 = BrokerConfiguration.builder()
				.port(100).build();

		webClient.put().uri("/mqtt/"+ BROKER_NAME )
				.contentType(MediaType.APPLICATION_JSON)
				.body(BodyInserters.fromObject(brokerConfiguration1))
				.exchange()
				.expectStatus()
				.isOk()
				.expectBody().isEmpty();
	}
	@Test
	public void shouldDeleteBrokerConfiguration() {
		when(brokerConfigRepository.deleteBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.empty());
		webClient
				.delete().uri("/mqtt/"+ BROKER_NAME)
				.exchange()
				.expectStatus()
				.isOk();
	}

	@Test
	public void shouldReturnBadRequest_WhenPayloadIsNull() {
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.empty())
				.exchange()
				.expectStatus()
				.isBadRequest();
	}

	@Test
	public void shouldReturnEmptyResponse_WhenPayloadIsEmpty() {
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(""))
				.exchange()
				.expectStatus()
				.isOk();
	}

	@Test
	public void shouldThrowException_WhenBrokerNtFound() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.empty());
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(MESSAGE_TO_THE_BORKER))
				.exchange()
				.expectBody(BrokerConfigException.class)
				.consumeWith(res -> {
					assertTrue(res.getResponseBody() instanceof BrokerConfigException);
				});

		webClient
				.get().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.exchange()
				.expectBody(BrokerConfigException.class)
				.consumeWith(res -> {
					assertTrue(res.getResponseBody() instanceof BrokerConfigException);
				});
	}

	@Test
	public void shouldThrowException_WhenReturnedClientIsEmpty() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));
		when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(null);
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(MESSAGE_TO_THE_BORKER))
				.exchange()
				.expectBody(BrokerConfigException.class)
				.consumeWith(res -> {
					assertTrue(res.getResponseBody() instanceof BrokerConfigException);
				});

		webClient
				.get().uri("/mqtt/"+ BROKER_NAME +"/get/"+ TOPIC_1)
				.exchange()
				.expectBody(BrokerConfigException.class)
				.consumeWith(res -> {
					assertTrue(res.getResponseBody() instanceof BrokerConfigException);
				});
	}

	@Test
	public void shouldPublishWthProperBrokerConfig() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));
		when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(mqtt5ReactorClient);
		when(clientConnectWorkFlow.connectSubscriber(mqtt5ReactorClient)).thenReturn(Mono.just(mqttConnAck));
		when(mqttConnAck.getReasonCode()).thenReturn(Mqtt5ConnAckReasonCode.SUCCESS);
		when(publisherWorkFlow.publishWorkFlow(mqtt5ReactorClient, MESSAGE_TO_THE_BORKER, TOPIC_1))
				.thenReturn(Flux.just(mqttPublishResult));
		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(MESSAGE_TO_THE_BORKER))
				.exchange()
				.expectStatus()
				.isOk();
	}

	@Test
	public void shouldThrowException_WhenAckFails() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));
		when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(mqtt5ReactorClient);
		when(clientConnectWorkFlow.connectSubscriber(mqtt5ReactorClient)).thenReturn(Mono.just(mqttConnAck));
		when(mqttConnAck.getReasonCode()).thenReturn(Mqtt5ConnAckReasonCode.BAD_AUTHENTICATION_METHOD);

		webClient
				.post().uri("/mqtt/"+ BROKER_NAME +"/send/"+ TOPIC_1)
				.body(BodyInserters.fromValue(MESSAGE_TO_THE_BORKER))
				.exchange()
				.expectBody(BrokerConfigException.class);

		webClient
				.get().uri("/mqtt/"+ BROKER_NAME +"/get/"+ TOPIC_1)
				.exchange()
				.expectBody(BrokerConfigException.class);
	}

	@Test
	public void shouldSubscribeWithProperBrokerConfig() {
		when(brokerConfigRepository.lookupBrokerConfiguration(BROKER_NAME)).thenReturn(Mono.just(brokerConfiguration));
		when(mqtt5ClientFactory.apply(brokerConfiguration)).thenReturn(mqtt5ReactorClient);
		when(clientConnectWorkFlow.connectSubscriber(mqtt5ReactorClient)).thenReturn(Mono.just(mqttConnAck));
		when(mqttConnAck.getReasonCode()).thenReturn(Mqtt5ConnAckReasonCode.SUCCESS);
		when(subscriptionWorkFlow.subscriptionWorkFlow(mqtt5ReactorClient, TOPIC_1))
				.thenReturn(Flux.just(MESSAGE_TO_THE_BORKER));
		webClient
				.get().uri("/mqtt/"+ BROKER_NAME +"/get/"+ TOPIC_1)
				.exchange()
				.expectStatus()
				.isOk()
				.expectBody()
				.consumeWith(entityExchangeResult -> {
					assertTrue(new String(entityExchangeResult.getResponseBody()).equals(MESSAGE_TO_THE_BORKER));
				});
	}
}
