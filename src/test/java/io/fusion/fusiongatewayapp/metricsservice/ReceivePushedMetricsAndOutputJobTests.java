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

package io.fusion.fusiongatewayapp.metricsservice;

import io.fusion.core.config.FusionDataServiceConfig;
import io.restassured.http.ContentType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("push")
@Testcontainers
class ReceivePushedMetricsAndOutputJobTests extends JobTestBase {
    @Value("http://localhost:${local.server.port}")
    String baseUrl;

    static int oispPort;

    @Autowired
    private FusionDataServiceConfig fusionDataServiceConfig;

    @BeforeEach
    void beforeEach() {
        oispPort = Integer.parseInt(fusionDataServiceConfig.getConnectionString().split(":")[1]);
    }

    @Test
    void normal() throws IOException {
        DatagramSocket socket = new DatagramSocket(oispPort);
        UdpPacketReceiver udpPacketReceiver = new UdpPacketReceiver(socket);
        new Thread(udpPacketReceiver).start();

        Map<String, String> sourceMetrics = new HashMap<>();
        sourceMetrics.put("pressure_bottle_right", "0.0");
        sourceMetrics.put("pressure_bottle_left", "140.6");
        sourceMetrics.put("E2MIX-Entnahmedruck_1", "12.01");

        udpPacketReceiver.reset();

        given()
                .contentType(ContentType.JSON)
                .body(sourceMetrics)

                .when()
                .post(baseUrl + "/betriebsdaten")

                .then()
                .statusCode(200);

        await().until(() -> udpPacketReceiver.getMessages().size() == 6);

        List<String> receivedMessages = udpPacketReceiver.getMessages().stream().map(this::normaliseNewlines).collect(Collectors.toList());

        List<String> expectedMessages = new ArrayList<>();
        expectedMessages.add("{\"n\":\"pressure_bottle_right\",\"t\":\"Druck.v1.0\"}");
        expectedMessages.add("{\"n\":\"pressure_takeoff_1\",\"t\":\"Druck.v1.0\"}");
        expectedMessages.add("{\"n\":\"pressure_bottle_left\",\"t\":\"Druck.v1.0\"}");
        expectedMessages.add("{\"n\":\"pressure_bottle_right\",\"v\":\"0.0\"}");
        expectedMessages.add("{\"n\":\"pressure_takeoff_1\",\"v\":\"12.01\"}");
        expectedMessages.add("{\"n\":\"pressure_bottle_left\",\"v\":\"140.6\"}");
        Assertions.assertThat(receivedMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);

        udpPacketReceiver.cancel();
        socket.close();
    }

    @Test
    void unknownMetrics() throws SocketException {
        DatagramSocket socket = new DatagramSocket(oispPort);
        UdpPacketReceiver udpPacketReceiver = new UdpPacketReceiver(socket);
        new Thread(udpPacketReceiver).start();

        Map<String, String> sourceMetrics = new HashMap<>();
        sourceMetrics.put("pressure_box", "0.0");
        sourceMetrics.put("pressure_sox", "140.6");
        sourceMetrics.put("pressure_bottle_left", "139.2");


        udpPacketReceiver.reset();

        given()
                .contentType(ContentType.JSON)
                .body(sourceMetrics)

                .when()
                .post(baseUrl + "/betriebsdaten")

                .then()
                .statusCode(200);

        await().until(() -> udpPacketReceiver.getMessages().size() == 2);

        List<String> receivedMessages = udpPacketReceiver.getMessages().stream().map(this::normaliseNewlines).collect(Collectors.toList());

        List<String> expectedMessages = new ArrayList<>();
        expectedMessages.add("{\"n\":\"pressure_bottle_left\",\"t\":\"Druck.v1.0\"}");
        expectedMessages.add("{\"n\":\"pressure_bottle_left\",\"v\":\"139.2\"}");

        assertThat(receivedMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);

        udpPacketReceiver.cancel();
        socket.close();
    }


}
