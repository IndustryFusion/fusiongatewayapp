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

import io.fusion.core.source.MetricsPushService;
import io.fusion.core.source.PushCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@Primary
public class GenericMetricsPushService implements MetricsPushService {
    private final Map<String, PushCallback> jobCallbacks = new HashMap<>();

    @Override
    public void start(String jobId, PushCallback pushCallback) {
        jobCallbacks.put(jobId, pushCallback);
    }

    @Override
    public void stop(String jobId) {
        jobCallbacks.remove(jobId);
    }

    public void stopAll() {
        jobCallbacks.clear();
    }

    public void handleMetrics(String jobId, Map<String, Object> metrics) {
        log.info("Handling {} metrics with job {}.", metrics.size(), jobId);
        final PushCallback pushCallback = jobCallbacks.get(jobId);
        if (pushCallback == null) {
            log.warn("Job not found {}!", jobId);
            return;
        }
        pushCallback.handleMetrics(jobId, metrics);
    }
}
