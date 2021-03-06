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
package org.apache.karaf.decanter.alerting.email;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.util.Dictionary;
import java.util.Properties;

@Component(
    name = "org.apache.karaf.decanter.alerting.email",
    property = EventConstants.EVENT_TOPIC + "=decanter/alert/*"
)
public class EmailAlerter implements EventHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(EmailAlerter.class);

    private String from;
    private String to;

    private Properties properties;

    @SuppressWarnings("unchecked")
    public void activate(ComponentContext context) throws ConfigurationException {
        Dictionary<String, Object> config = context.getProperties();
        requireProperty(config, "from");
        requireProperty(config, "to");
        requireProperty(config, "host");

        this.from = (String) config.get("from");
        this.to = (String) config.get("to");

        properties = new Properties();
        properties.put("mail.smtp.host", config.get("host"));
        properties.put("mail.smtp.port", config.get("port"));
        properties.put("mail.smtp.auth", config.get("auth"));
        properties.put("mail.smtp.starttls.enable", config.get("starttls"));
        properties.put("mail.smtp.ssl.enable", config.get("ssl"));
        String username = (String) config.get("username");
        String password = (String) config.get("password");
        if (username != null) {
            properties.put("mail.smtp.user", username);
        }
        if (password != null) {
            properties.put("mail.smtp.password", password);
        }
    }

    private void requireProperty(Dictionary<String, ?> config, String key) throws ConfigurationException {
        if (config.get(key) == null) {
            throw new ConfigurationException(key, key + " property is not defined");
        }
    }

    @Override
    public void handleEvent(Event event) {
        Session session = Session.getDefaultInstance(properties);
        MimeMessage message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress(from));
            message.addRecipients(Message.RecipientType.TO, to);
            String alertLevel = (String) event.getProperty("alertLevel");
            String alertAttribute = (String) event.getProperty("alertAttribute");
            String alertPattern = (String) event.getProperty("alertPattern");
            boolean recovery = (boolean) event.getProperty("alertBackToNormal");
            if (!recovery) {
                message.setSubject("[" + alertLevel + "] Alert on " + alertAttribute);
            } else {
                message.setSubject("Alert on " + alertAttribute + " back to normal");
            }
            StringBuilder builder = new StringBuilder();
            if (!recovery) {
                builder.append(alertLevel + " alert: " + alertAttribute + " is out of the pattern " + alertPattern + "\n");
            } else {
                builder.append(alertLevel + " alert: " + alertAttribute + " was out of the pattern " + alertPattern + ", but back to normal now\n");
            }
            builder.append("\n");
            builder.append("Details:\n");
            for (String name : event.getPropertyNames()) {
                builder.append("\t").append(name).append(": ").append(event.getProperty(name)).append("\n");
            }
            message.setText(builder.toString());
            if (properties.get("mail.smtp.user") != null) {
                Transport.send(message, (String) properties.get("mail.smtp.user"), (String) properties.get("mail.smtp.password"));
            } else {
                Transport.send(message);
            }
        } catch (Exception e) {
            LOGGER.error("Can't send the alert e-mail", e);
        }
    }

}
