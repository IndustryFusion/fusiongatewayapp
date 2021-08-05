/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.fusion.fusiongatewayapp.outputservice;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.fusion.core.config.FusionDataServiceConfig;
import io.fusion.core.exception.ConfigurationException;
import io.fusion.core.output.OutputService;
import io.fusion.fusiongatewayapp.config.FusionGatewayAppConfig;
import io.fusion.fusiongatewayapp.exception.UdpException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
@Primary
public class OispOutputService implements OutputService {
    private final FusionGatewayAppConfig fusionGatewayAppConfig;
    private final FusionDataServiceConfig fusionDataServiceConfig;

    private final Gson gson = new Gson();
    private DatagramSocket socket;
    private SocketAddress agentAddress;

    @Autowired
    public OispOutputService(final FusionGatewayAppConfig fusionGatewayAppConfig,
                             FusionDataServiceConfig fusionDataServiceConfig) {
        this.fusionGatewayAppConfig = fusionGatewayAppConfig;
        this.fusionDataServiceConfig = fusionDataServiceConfig;
    }

    @PostConstruct
    public void init() {
        final String connectionString = fusionDataServiceConfig.getConnectionString();
        final String[] parts = connectionString.split(":");
        if (parts.length != 2) {
            throw new ConfigurationException();
        }

        SocketAddress localAddress = new InetSocketAddress(0);
        try {
            socket = new DatagramSocket(localAddress);
        } catch (SocketException e) {
            throw new UdpException("Socket init", e);
        }
        agentAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    }

    private String generateComponentRegistration(String metricName, String metricType) {
        JsonObject componentRegistration = new JsonObject();
        componentRegistration.addProperty("n", metricName);
        componentRegistration.addProperty("t", metricType);
        return gson.toJson(componentRegistration) + "\n";
    }

    private String generatePayload(String metricName, String metricValue) {
        JsonObject metric = new JsonObject();
        metric.addProperty("n", metricName);
        if ("true".equals(metricValue) || "false".equals(metricValue)) {
            metric.addProperty("v", Boolean.parseBoolean(metricValue));
        } else {
            metric.addProperty("v", metricValue);
        }
        return gson.toJson(metric) + "\n";
    }

    @Override
    public void sendMetrics(String jobId, Map<String, Object> metrics) {
        if (metrics != null) {
            Map<String, String> components = fusionGatewayAppConfig.getComponentMap().entrySet().stream()
                    .filter(entry -> metrics.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            components.forEach((name, type) -> {
                String payload = generateComponentRegistration(name, type);
                log.info("Sending componentRegistration <{}> to {}", payload, agentAddress);

                byte[] buf = payload.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(buf, buf.length, agentAddress);
                try {
                    socket.send(packet);
                } catch (IOException e) {
                    throw new UdpException("Socket send: componentRegistration", e);
                }
            });

            metrics.forEach((name, value) -> {
                String payload = generatePayload(name, value.toString());
                log.info("Sending payload <{}> to {}", payload, agentAddress);

                byte[] buf = payload.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(buf, buf.length, agentAddress);
                try {
                    socket.send(packet);
                } catch (IOException e) {
                    throw new UdpException("Socket send: metric", e);
                }
            });
        }
    }
}
