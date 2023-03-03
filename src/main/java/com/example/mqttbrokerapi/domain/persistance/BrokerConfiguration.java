package com.example.mqttbrokerapi.domain.persistance;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BrokerConfiguration {
    private String hostName;
    private int port;
}
