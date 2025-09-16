/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.karaf.decanter.collector.cxf;

import static org.apache.karaf.decanter.collector.cxf.DecanterLoggingInInterceptor.DECANTER_CORRELATION_ID;

import java.util.Map;

import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.transport.http.AbstractHTTPDestination;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletResponse;

public class DecanterLoggingOutInterceptor extends AbstractPhaseInterceptor<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(DecanterLoggingOutInterceptor.class);
    public static final String DECANTER_COLLECT_CXF_TOPIC = "decanter/collect/cxf";

    private final EventAdmin dispatcher;
    private final Map<String, Map<String, Object>> requestMap;

    public DecanterLoggingOutInterceptor(EventAdmin dispatcher, Map<String, Map<String, Object>> requestMap) {
        super(Phase.POST_STREAM);
        this.dispatcher = dispatcher;
        this.requestMap = requestMap;
    }

    @Override
    public void handleMessage(Message message) throws Fault {
        // Only process if this is an HTTP request
        if (!isHttpRequest(message)) {
            return;
        }

        try {
            String correlationId = (String) message.getExchange().get(DECANTER_CORRELATION_ID);
            Map<String, Object> eventData = requestMap.remove(correlationId);
            if (eventData == null) {
                LOG.debug("event Data not found in the map");
                return;
            }
            HttpServletResponse response =
                    (HttpServletResponse) message.get(AbstractHTTPDestination.HTTP_RESPONSE);

            eventData.put("response.status", message.get(org.apache.cxf.message.Message.RESPONSE_CODE));

            for (String headerName : response.getHeaderNames()) {
                eventData.put("response.header." + headerName, response.getHeader(headerName));
            }

            eventData.put("response.contentType", response.getContentType());
            eventData.put("response.characterEncoding", response.getCharacterEncoding());

            Long start = (Long) eventData.remove("timestamp");
            eventData.put("response.elapseTime", System.currentTimeMillis() - start);

            // Send event to Decanter
            sendDecanterEvent(eventData);

        } catch (Exception e) {
            LOG.warn("Failed to log CXF request: {}", e.getMessage());
        }
    }

    private boolean isHttpRequest(Message message) {
        return message.get(AbstractHTTPDestination.HTTP_RESPONSE) != null;
    }


    private void sendDecanterEvent(Map<String, Object> eventData) {
        if (dispatcher != null) {
            LOG.debug("data sent {}", eventData);
            org.osgi.service.event.Event event = new org.osgi.service.event.Event(DECANTER_COLLECT_CXF_TOPIC, eventData);
            dispatcher.postEvent(event);
        }
    }
}
