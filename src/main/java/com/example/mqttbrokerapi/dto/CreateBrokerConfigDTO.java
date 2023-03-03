package com.example.mqttbrokerapi.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CreateBrokerConfigDTO {
    @JsonProperty("host_name")
    private String hostName;
    private int port;
}
