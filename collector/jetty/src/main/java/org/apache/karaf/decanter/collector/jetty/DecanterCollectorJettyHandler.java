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
package org.apache.karaf.decanter.collector.jetty;

import static org.eclipse.jetty.server.Request.getTimeStamp;

import org.apache.karaf.decanter.collector.utils.PropertiesPreparator;
import org.eclipse.jetty.ee10.servlet.ServletCoreRequest;
import org.eclipse.jetty.ee10.servlet.ServletCoreResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Session;
import org.eclipse.jetty.util.Callback;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.event.EventConstants;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component(
        name = "org.apache.karaf.decanter.collector.jetty",
        service = { Handler.class },
        immediate = true
)
public class DecanterCollectorJettyHandler implements Handler {

    @Reference
    public EventAdmin dispatcher;

    private Server server;

    private boolean started = false;
    private Dictionary<String, Object> properties;

    @Activate
    public void activate(ComponentContext componentContext) {
        this.properties = componentContext.getProperties();
    }

    @Override
    public void start() throws Exception {
        started = true;
    }

    @Override
    public void stop() throws Exception {
        started = false;
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isStarting() {
        return false;
    }

    @Override
    public boolean isStopping() {
        return false;
    }

    @Override
    public boolean isStopped() {
        return !started;
    }

    @Override
    public boolean isFailed() {
        return false;
    }

    @Override
    public boolean addEventListener(EventListener listener) {
        return false;
    }

    @Override
    public boolean removeEventListener(EventListener listener) {
        return false;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
        Map<String, Object> data = new HashMap<>();
        // SUPPORT-5718
        long startTime = getTimeStamp(request);
        data.put("type", "jetty");
        data.put("request.method", request.getMethod());
        data.put("request.requestURI", request.getHttpURI().getPath());
        try {
            Session session = request.getSession(true);
            if (session != null) {
                data.put("request.session.id", session.getId());
            }
        } catch (Exception e) {
            // nothing to do
        }
        data.put("request.contentType", request.getHeaders().get(HttpHeader.CONTENT_TYPE));
        AuthenticationState.Succeeded succeededAuthentication = getSucceededAuthentication(request);
        data.put("request.authType", succeededAuthentication != null
                ? succeededAuthentication.getAuthenticationType() : "none");
        data.put("request.contextPath", Request.getContextPath(request));
        ServletCoreRequest coreRequest = Request.as(request, ServletCoreRequest.class);
        HttpServletRequest servletRequest = coreRequest != null ? coreRequest.getServletRequest() : null;
        if (servletRequest != null) {
            data.put("request.pathInfo", servletRequest.getPathInfo());
            data.put("request.pathTranslated", servletRequest.getPathTranslated());
        }
        data.put("request.queryString", request.getHttpURI().getQuery());
        data.put("request.remoteUser", succeededAuthentication != null
                ? succeededAuthentication.getUserPrincipal().getName() : "none");
        if (servletRequest != null) {
            data.put("request.requestedSessionId", servletRequest.getRequestedSessionId());
        }
        data.put("request.requestURL", getRequestURL(request));
        if (servletRequest != null) {
            data.put("request.servletPath", servletRequest.getServletPath());
        }
        data.put("request.localAddr", Request.getLocalAddr(request));
        for (String name : request.getAttributeNameSet()) {
            data.put("request.attribute." + name, request.getAttribute(name));
        }
        if (servletRequest != null) {
            Enumeration<String> parameterNames = servletRequest.getParameterNames();
            while (parameterNames.hasMoreElements()) {
                String name = parameterNames.nextElement();
                data.put("request.parameter." + name, servletRequest.getParameter(name));
            }
        }
        for (String name : request.getHeaders().getFieldNamesCollection()) {
            data.put("request.header." + name, request.getHeaders().get(name));
        }
        ServletCoreResponse coreResponse = Response.as(response, ServletCoreResponse.class);
        HttpServletResponse servletResponse = coreResponse != null ? coreResponse.getServletResponse() : null;
        data.put("response.status", response.getStatus());
        for (String name : response.getHeaders().getFieldNamesCollection()) {
            data.put("response.header." + name, response.getHeaders().get(name));
        }
        data.put("response.contentType", response.getHeaders().get(HttpHeader.CONTENT_TYPE));
        if (servletResponse != null) {
            data.put("response.characterEncoding", servletResponse.getCharacterEncoding());
        }

        // SUPPORT-5718
        if (servletRequest.getReader() != null) {
            data.put("request.reader", servletRequest.getReader().lines().collect(Collectors.joining(System.lineSeparator())));
        }
        long endTime = System.currentTimeMillis();
        long elapseTime = endTime - startTime;
        data.put("response.elapseTime", elapseTime);
        try {
            PropertiesPreparator.prepare(data, properties);
        } catch (Exception e) {
            // nothing to do
        }
        String topic = (properties.get(EventConstants.EVENT_TOPIC) != null) ? (String) properties.get(EventConstants.EVENT_TOPIC) : "decanter/collect/jetty";
        Event event = new Event(topic, data);
        dispatcher.postEvent(event);
        callback.succeeded();
        return true;
    }

    @Override
    public void setServer(Server server) {
        this.server = server;
    }

    @Override
    public Server getServer() {
        return server;
    }

    @Override
    public void destroy() {
        // nothing to do
    }

    private static AuthenticationState.Succeeded getSucceededAuthentication(Request request) {
        AuthenticationState authenticationState = AuthenticationState.getAuthenticationState(request);
        if (authenticationState instanceof AuthenticationState.Deferred) {
            AuthenticationState.Deferred deferred = (AuthenticationState.Deferred) authenticationState;
            AuthenticationState undeferred = deferred.authenticate(request);
            if (undeferred != null && undeferred != authenticationState) {
                authenticationState = undeferred;
                AuthenticationState.setAuthenticationState(request, authenticationState);
            }
        }
        if (authenticationState instanceof AuthenticationState.Succeeded) {
            return (AuthenticationState.Succeeded) authenticationState;
        }
        return null;
    }

    private static String getRequestURL(Request request) {
        return HttpURI.build(request.getHttpURI()).query(null).asString();
    }
}
