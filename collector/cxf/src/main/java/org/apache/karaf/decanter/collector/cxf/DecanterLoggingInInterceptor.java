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

import jakarta.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.io.CachedOutputStream;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.transport.http.AbstractHTTPDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DecanterLoggingInInterceptor extends AbstractPhaseInterceptor<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(DecanterLoggingInInterceptor.class.getName());
    public static final String DECANTER_CORRELATION_ID = "DECANTER_CORRELATION_ID";

    private final Map<String, Map<String, Object>> requestMap;

    private static final int LIMIT = 64 * 1024; // 64KB default limit

    public DecanterLoggingInInterceptor(Map<String, Map<String, Object>> requestMap) {
        super(Phase.RECEIVE);
        this.requestMap = requestMap;
    }

    @Override
    public void handleMessage(Message message) throws Fault {
        // Only process if this is an HTTP request
        if (!isHttpRequest(message)) {
            return;
        }

        try {
            // Extract HTTP request details
            HttpServletRequest request = (HttpServletRequest) message.get(AbstractHTTPDestination.HTTP_REQUEST);
            String requestUri = (String) message.get(Message.REQUEST_URI);
            String httpMethod = (String) message.get(Message.HTTP_REQUEST_METHOD);

            // Capture the request body
            String requestBody = captureRequestBody(message);

            // Create Decanter event
            Map<String, Object> eventData = new HashMap<>();
            eventData.put("type", "cxf-request");
            eventData.put("timestamp", System.currentTimeMillis());

            // HTTP details
            eventData.put("http.method", httpMethod);
            eventData.put("http.uri", requestUri);
            eventData.put("http.remote.address", request.getRemoteAddr());
            eventData.put("http.contentType", request.getContentType());
            eventData.put("http.content.length", request.getContentLength());

            // Request body
            if (requestBody != null && !requestBody.isEmpty()) {
                eventData.put("request.reader", requestBody);
            }

            // Headers
            Map<String, Object> headers = new HashMap<>();
            java.util.Enumeration<String> headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                headers.put(headerName, request.getHeader(headerName));
            }
            eventData.put("http.request.headers", headers);

            // Exchange ID for correlation
            String exchangeId = (String) message.getExchange().get("ExchangeId");
            if (exchangeId != null) {
                eventData.put("exchange.id", exchangeId);
            }
            String correlationId = UUID.randomUUID().toString();

            requestMap.put(correlationId, eventData);
            message.getExchange().put(DECANTER_CORRELATION_ID,correlationId);


        } catch (Exception e) {
            LOG.warn("Failed to log CXF request: " + e.getMessage());
        }
    }

    private boolean isHttpRequest(Message message) {
        return message.get(AbstractHTTPDestination.HTTP_REQUEST) != null;
    }

    private String captureRequestBody(Message message) {
        try {
            InputStream inputStream = message.getContent(InputStream.class);
            if (inputStream == null) {
                return null;
            }

            // Create cached output stream to capture content
            CachedOutputStream cos = new CachedOutputStream();

            // Copy input stream to cached stream
            copyStream(inputStream, cos, LIMIT);

            // Reset the message content with cached stream
            message.setContent(InputStream.class, cos.getInputStream());
            cos.close();

            return cos.getOut().toString();

        } catch (Exception e) {
            LOG.warn("Failed to capture request body: " + e.getMessage());
            return null;
        }
    }

    private void copyStream(InputStream input, OutputStream output, int limit) throws IOException {
        byte[] buffer = new byte[8192];
        int bytesRead;
        int totalBytes = 0;

        while ((bytesRead = input.read(buffer)) != -1 && totalBytes < limit) {
            int bytesToWrite = Math.min(bytesRead, limit - totalBytes);
            output.write(buffer, 0, bytesToWrite);
            totalBytes += bytesToWrite;
        }
    }

}
