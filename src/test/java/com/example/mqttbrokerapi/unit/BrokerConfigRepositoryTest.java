package com.example.mqttbrokerapi.unit;

import static org.junit.Assert.assertEquals;

import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import com.example.mqttbrokerapi.domain.persistance.impl.BrokerConfigRepositoryImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
public class BrokerConfigRepositoryTest {
	public BrokerConfigRepositoryImpl brokerConfigRepository;

	private final static int SSL_PORT = 8883;
	private final static String HOST_NAME = "sample.s2.eu.hivemq.cloud";

	private BrokerConfiguration brokerConfiguration;
	@BeforeEach
	public void setUp() {
		brokerConfigRepository = new BrokerConfigRepositoryImpl();
		brokerConfiguration =  BrokerConfiguration.builder()
				.port(SSL_PORT)
				.hostName(HOST_NAME)
				.build();
	}

	@Test
	public void shouldPutBrokerConfiguration() {
		assertEquals(brokerConfigRepository.persistBrokerConfiguration("Broker", brokerConfiguration), Mono.empty());
	}

	@Test
	public void shouldGetBrokerConfiguration() {
		brokerConfigRepository.persistBrokerConfiguration("Broker", brokerConfiguration);
		assertEquals(brokerConfigRepository.lookupBrokerConfiguration("Broker").block(), brokerConfiguration);
	}

	@Test
	public void shouldDeleteBrokerConfiguration() {
		brokerConfigRepository.persistBrokerConfiguration("Broker", brokerConfiguration);
		assertEquals(brokerConfigRepository.deleteBrokerConfiguration("Broker"), Mono.empty());
	}

	@Test
	public void shouldReturnEmptyBrokerConfiguration() {
		assertEquals(brokerConfigRepository.lookupBrokerConfiguration("Broker2"), Mono.empty());
	}

	@Test
	public void shouldReturnEmptyOnDeletion_WhenBrokerConfigNotExists() {
		assertEquals(brokerConfigRepository.deleteBrokerConfiguration("Broker2"), Mono.empty());
	}
	@Test
	public void shouldRturnEmpty_WhenBrokerConfigNameIsEmptyorNull() {
		assertEquals(brokerConfigRepository.persistBrokerConfiguration("", brokerConfiguration), Mono.empty());

		assertEquals(brokerConfigRepository.persistBrokerConfiguration(null, brokerConfiguration), Mono.empty());
	}
}
