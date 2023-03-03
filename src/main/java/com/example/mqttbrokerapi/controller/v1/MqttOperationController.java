package com.example.mqttbrokerapi.controller.v1;

import java.util.Objects;
import java.util.function.Function;

import com.example.mqttbrokerapi.domain.persistance.BrokerConfigRepository;
import com.example.mqttbrokerapi.domain.persistance.BrokerConfiguration;
import com.example.mqttbrokerapi.dto.CreateBrokerConfigDTO;
import com.example.mqttbrokerapi.exception.BrokerConfigException;
import com.example.mqttbrokerapi.service.ClientConnectWorkFlow;
import com.example.mqttbrokerapi.service.PublisherWorkFlow;
import com.example.mqttbrokerapi.service.SubscriptionWorkFlow;
import com.example.mqttbrokerapi.util.Message;
import com.google.common.base.Strings;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/mqtt")
public class MqttOperationController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttOperationController.class);
    private final Function<BrokerConfiguration, Mqtt5ReactorClient> mqtt5ClientFactory;
    private final SubscriptionWorkFlow subscriptionWorkFlow;
    private final ClientConnectWorkFlow clientConnectWorkFlow;
    private final PublisherWorkFlow publisherWorkFlow;
    private final BrokerConfigRepository brokerConfigRepository;

    public MqttOperationController(BrokerConfigRepository brokerConfigRepository,
            Function<BrokerConfiguration, Mqtt5ReactorClient> mqtt5ClientFactory, SubscriptionWorkFlow subscriptionWorkFlow,
            ClientConnectWorkFlow clientConnectWorkFlow, PublisherWorkFlow publisherWorkFlow) {
        this.brokerConfigRepository = brokerConfigRepository;
        this.mqtt5ClientFactory = mqtt5ClientFactory;
        this.subscriptionWorkFlow = subscriptionWorkFlow;
        this.clientConnectWorkFlow = clientConnectWorkFlow;
        this.publisherWorkFlow = publisherWorkFlow;
    }

    @PutMapping("/{broker-name}")
    public Mono<Void> putBrokerConfiguration(@PathVariable("broker-name") final String brokerName,
                                             final @RequestBody CreateBrokerConfigDTO createBrokerConfigDTO) {
        Objects.requireNonNull(brokerName);
        Objects.requireNonNull(createBrokerConfigDTO);

        if(Strings.isNullOrEmpty(createBrokerConfigDTO.getHostName())) {
            return Mono.empty();
        }

        final BrokerConfiguration brokerConfiguration = BrokerConfiguration.builder()
                .hostName(createBrokerConfigDTO.getHostName())
                .port(createBrokerConfigDTO.getPort()).build();
        return brokerConfigRepository.persistBrokerConfiguration(brokerName, brokerConfiguration);
    }

    @GetMapping("/{broker-name}")
    public Mono<BrokerConfiguration> putBrokerConfiguration(@PathVariable("broker-name") final String brokerName) {
        Objects.requireNonNull(brokerName);
        return brokerConfigRepository.lookupBrokerConfiguration(brokerName);
    }

    @DeleteMapping("/{broker-name}")
    public Mono<Void> deleteBrokerConfiguration(@PathVariable("broker-name") final String brokerName) {
        Objects.requireNonNull(brokerName);
        return brokerConfigRepository.deleteBrokerConfiguration(brokerName);
    }

    @PostMapping("/{broker-name}/send/{topic-name}")
    public Mono<Void> publishToTheBroker(@PathVariable("broker-name") final String brokerName,
                                         @PathVariable("topic-name") final String topicName,@RequestBody String message) {
        Objects.requireNonNull(brokerName);
        if(Strings.isNullOrEmpty(message)) {
            return Mono.empty();
        }
        return brokerConfigRepository
                .lookupBrokerConfiguration(brokerName)
                .switchIfEmpty(Mono.error(new BrokerConfigException(Message.INVALID_BROKER)))
                .map(mqtt5ClientFactory)
                .switchIfEmpty(Mono.error(new BrokerConfigException(Message.INVALID_BROKER)))
                .flatMap(mqtt5ReactorClient -> clientConnectWorkFlow.connectSubscriber(mqtt5ReactorClient).doOnSuccess(theConnectionAck -> {
                    if (theConnectionAck != null) {
                        final Mqtt5ConnAckReasonCode theConnectionAckReasonCode =
                                theConnectionAck.getReasonCode();
                        LOGGER.info("Connecting publisher received ACK code: {}", theConnectionAckReasonCode);
                         publisherWorkFlow.publishWorkFlow(mqtt5ReactorClient, message, topicName).subscribe();
                    }
                }).doOnError(throwable -> new BrokerConfigException(throwable.getMessage())).then()).then();
    }

    @GetMapping("/{broker-name}/get/{topic-name}")
    public Flux<String> subscribeFromTheBroker(@PathVariable("broker-name") final String brokerName,
                                         @PathVariable("topic-name") final String topicName) {
        Objects.requireNonNull(brokerName);
         return brokerConfigRepository
                .lookupBrokerConfiguration(brokerName)
                 .switchIfEmpty(Mono.error(new BrokerConfigException(Message.INVALID_BROKER)))
                .map(mqtt5ClientFactory)
                 .switchIfEmpty(Mono.error(new BrokerConfigException(Message.INVALID_BROKER)))
                 .flatMapMany(mqtt5ReactorClient ->
                         clientConnectWorkFlow.connectSubscriber(mqtt5ReactorClient)
                         .doOnSuccess(theConnectionAck -> {
                             if (theConnectionAck != null) {
                                 final Mqtt5ConnAckReasonCode theConnectionAckReasonCode =
                                         theConnectionAck.getReasonCode();
                                 LOGGER.info("Connecting subscriber to broker received ACK code: {}", theConnectionAckReasonCode);
                             }
                         })
                         .retry(3)
                         .doOnError(throwable -> new BrokerConfigException(throwable.getMessage()))
                         .flatMapMany(mqtt5ConnAck ->
                                 subscriptionWorkFlow
                                         .subscriptionWorkFlow(mqtt5ReactorClient, topicName)
                                 .share()
                                 .doOnSubscribe(subscription -> LOGGER.debug("Client subscribed"))
                                 .doOnCancel(() -> LOGGER.debug("Subscription cancelled"))));
    }



}
