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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cxf.BusFactory;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.event.EventAdmin;

public class Activator implements BundleActivator {


    private DecanterLoggingInInterceptor loggingInInterceptor;
    private DecanterLoggingOutInterceptor loggingOutInterceptor;

    private final Map<String, Map<String, Object> > requestMap = new ConcurrentHashMap<>();

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        ServiceReference<EventAdmin> ref = bundleContext.getServiceReference(EventAdmin.class);
        EventAdmin eventAdmin = null;
        if (ref != null) {
            eventAdmin = bundleContext.getService(ref);
        }
        loggingInInterceptor = new DecanterLoggingInInterceptor(requestMap);
        loggingOutInterceptor = new DecanterLoggingOutInterceptor(eventAdmin, requestMap);
        BusFactory.getDefaultBus().getInInterceptors().add(loggingInInterceptor);
        BusFactory.getDefaultBus().getOutInterceptors().add(loggingOutInterceptor);
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        BusFactory.getDefaultBus().getInInterceptors().remove(loggingInInterceptor);
        BusFactory.getDefaultBus().getOutInterceptors().remove(loggingOutInterceptor);
        requestMap.clear();
    }
}
