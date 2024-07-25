/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.http;

import java.io.File ;
import java.io.IOException ;

import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.servlet.ServletOutputStream ;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest ;
import jakarta.servlet.http.HttpServletResponse ;
import org.apache.jena.riot.WebContent ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaConst ;
import org.slf4j.Logger ;

/** Respond with Prometheus metrics */
public class S_Metrics extends HttpServlet {
    static private Logger LOG = Delta.DELTA_LOG ;
    private PrometheusMeterRegistry meterRegistry;

    public S_Metrics() {
        meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        meterRegistry.config().commonTags("application", DeltaConst.pDeltaStore);

        new FileDescriptorMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new ClassLoaderMetrics().bindTo(meterRegistry);
        new UptimeMetrics().bindTo(meterRegistry);
        for ( File root : File.listRoots() ) {
            new DiskSpaceMetrics(root).bindTo(meterRegistry);
        }
        // Has a warning about resource closing.
        @SuppressWarnings("resource")
        JvmGcMetrics x = new JvmGcMetrics();
        x.bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        text(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        text(req, resp);
    }

    protected void metrics(ServletOutputStream out) throws IOException {
        out.write(meterRegistry.scrape().getBytes());
    }

    private void text(HttpServletRequest req, HttpServletResponse resp) {
        try {
            resp.setHeader(HttpNames.hContentType,  WebContent.contentTypeTextPlain);
            resp.setStatus(HttpSC.OK_200);
            try(ServletOutputStream out = resp.getOutputStream(); ) {
                metrics(out);
            }
        } catch (IOException ex) {
            LOG.warn("text out: IOException", ex);
            try {
                resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Internal server error");
            } catch (IOException ex2) {}
        }
    }
}
