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
import java.util.Enumeration;
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

            // Capture the request body
            byte[] requestBody = captureRequestBody(message);

            // Create Decanter event
            Map<String, Object> eventData = new HashMap<>();
            eventData.put("timestamp", System.currentTimeMillis());
            eventData.put("component.name","org.apache.karaf.decanter.collector.cxf");

            eventData.put("request.method", request.getMethod());
            eventData.put("request.requestURI", request.getRequestURI());

            try {
                if (request.getSession() != null) {
                    eventData.put("request.session.id", request.getSession().getId());
                }
            } catch (Exception var18) {
            }

            eventData.put("request.contentType", request.getContentType());
            eventData.put("request.authType", request.getAuthType());
            eventData.put("request.contextPath", request.getContextPath());
            eventData.put("request.pathInfo", request.getPathInfo());
            eventData.put("request.pathTranslated", request.getPathTranslated());
            eventData.put("request.queryString", request.getQueryString());
            eventData.put("request.remoteUser", request.getRemoteUser());
            eventData.put("request.requestedSessionId", request.getRequestedSessionId());
            eventData.put("request.requestURL", request.getRequestURL());
            eventData.put("request.servletPath", request.getServletPath());
            eventData.put("request.localAddr", request.getLocalName());
            eventData.put("request.hostName", request.getLocalAddr());
            eventData.put("org.apache.cxf.message.Message.BASE_PATH", message.get("org.apache.cxf.message.Message.BASE_PATH"));

            Enumeration<String> attributeNames = request.getAttributeNames();

            while(attributeNames.hasMoreElements()) {
                String name = attributeNames.nextElement();
                eventData.put("request.attribute." + name, request.getAttribute(name));
            }

            Enumeration<String> parameterNames = request.getParameterNames();

            while(parameterNames.hasMoreElements()) {
                String name = parameterNames.nextElement();
                eventData.put("request.parameter." + name, request.getParameter(name));
            }

            Enumeration<String> requestHeaders = request.getHeaderNames();

            while(requestHeaders.hasMoreElements()) {
                String name = requestHeaders.nextElement();
                eventData.put("request.header." + name, request.getHeader(name));
            }

            // Request body
            if (requestBody != null && requestBody.length != 0) {
                eventData.put("request.reader", new String(requestBody));
                eventData.put("request.body.bytes", requestBody);
            }

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

    private byte[] captureRequestBody(Message message) {
        try {
            InputStream inputStream = message.getContent(InputStream.class);
            if (inputStream == null) {
                return null;
            }

            // Create cached output stream to capture content
            CachedOutputStream cos = new CachedOutputStream();

            // Copy input stream to cached stream
            copyStream(inputStream, cos, LIMIT);

            byte[] bytes;
            try (InputStream cachedIn = cos.getInputStream()) {
                bytes = cachedIn.readAllBytes();
            }

            // Reset the message content with cached stream
            message.setContent(InputStream.class, cos.getInputStream());
            cos.close();

            return bytes;

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
