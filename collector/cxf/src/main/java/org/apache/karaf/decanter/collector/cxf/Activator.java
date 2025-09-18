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

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.message.Message;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.event.EventAdmin;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator {


    private DecanterLoggingInInterceptor loggingInInterceptor;
    private DecanterLoggingOutInterceptor loggingOutInterceptor;
    private ServiceTracker<Bus, Bus> busTracker;
    private final Map<String, Map<String, Object>> requestMap = new ConcurrentHashMap<>();

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        ServiceReference<EventAdmin> ref = bundleContext.getServiceReference(EventAdmin.class);
        EventAdmin eventAdmin = ref != null ? bundleContext.getService(ref) : null;

        loggingInInterceptor = new DecanterLoggingInInterceptor(requestMap);
        loggingOutInterceptor = new DecanterLoggingOutInterceptor(eventAdmin, requestMap);

        busTracker = new ServiceTracker<>(bundleContext, Bus.class, new ServiceTrackerCustomizer<Bus, Bus>() {
            @Override
            public Bus addingService(ServiceReference<Bus> reference) {
                Bus bus = bundleContext.getService(reference);
                registerInterceptors(bus);
                return bus;
            }

            @Override
            public void modifiedService(ServiceReference<Bus> reference, Bus service) {
                //do nothing
            }

            @Override
            public void removedService(ServiceReference<Bus> reference, Bus service) {
                unregisterInterceptors(service);
                bundleContext.ungetService(reference);
            }
        });
        busTracker.open();

        Bus defaultBus = BusFactory.getDefaultBus();
        registerInterceptors(defaultBus);
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        if (busTracker != null) {
            busTracker.close();
            busTracker = null;
        }
        unregisterInterceptors(BusFactory.getDefaultBus());
        requestMap.clear();
    }

    private void registerInterceptors(Bus bus) {
        if (bus == null) return;
        addIfAbsent(bus.getInInterceptors(), loggingInInterceptor);
        addIfAbsent(bus.getOutInterceptors(), loggingOutInterceptor);
    }

    private void unregisterInterceptors(Bus bus) {
        if (bus == null) {
            return;
        }
        bus.getInInterceptors().remove(loggingInInterceptor);
        bus.getOutInterceptors().remove(loggingOutInterceptor);
    }

    private <T extends Interceptor<? extends Message>> void addIfAbsent(java.util.List<Interceptor<? extends Message>> list, T interceptor) {
        if (!list.contains(interceptor)) {
            list.add(interceptor);
        }
    }
}
