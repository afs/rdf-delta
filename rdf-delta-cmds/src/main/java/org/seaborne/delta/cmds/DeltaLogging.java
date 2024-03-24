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

package org.seaborne.delta.cmds;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.atlas.io.IOX;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.LogCtlLog4j2;

/**
 * Initialize logging for Delta.
 *
 * This is the implementation of logging backing slf4j.
 *
 * This always sets some kind of logging and does not output warning or error messages
 * from the logging implementation.
 */
public class DeltaLogging {
    private static String log4j2SysProp = "log4j.configurationFile";

    private static boolean INITIALIZED = false;

    public static void setLogging() {
        setLogging(false);
    }

    public static void setLogging(boolean withWarnings) {
        if ( INITIALIZED )
            return ;
        INITIALIZED = true;

        // Log4j2 configuration set from outside. Leave to log4j2 itself.
        if ( System.getProperty("log4j.configurationFile") != null )
            return;

        // See https://logging.apache.org/log4j/2.0/manual/configuration.html
        // Modify so that just the presence of a file of the right name will configure logging.
        // This helps for sealed jars (i.e. "java -jar").
        // If a log4j2 file is present use that:
        String[] log4j2files = { "log4j2.properties", "log4j2.yaml", "log4j2.yml", "log4j2.json", "log4j2.jsn", "log4j2.xml" };
        for ( String fn : log4j2files ) {
            if ( FileOps.exists(fn) ) {
                // Let Log4j2 initialize normally.
                System.setProperty("log4j.configurationFile", fn);
                return;
            }
        }

        if ( withWarnings ) {
            // Check for attempts at JUL and log4j1 setup.
            if ( FileOps.exists("logging.properties") ) {
                System.err.println("RDF Delta 0.7.0 and later uses log4j2 for logging");
                System.err.println("  Found 'logging.properties' (for java.util.logging) - ignored");
            }

            if ( FileOps.exists("log4j.properties") ) {
                System.err.println("RDF Delta 0.7.0 and later uses log4j2 for logging");
                System.err.println("  Found 'log4j.properties' (for log4j1) - ignored");
            }
        }

        // Else a default.
        defaultLogging();
    }

    /** Initialize log4j2 from a default (in XML non-strict format) */
    private static void defaultLogging() {
        byte b[] = StrUtils.asUTF8bytes(getDefaultString());
        try (InputStream input = new ByteArrayInputStream(b)) {
            LogCtlLog4j2.resetLogging(input, ".properties");
        }
        catch (IOException ex) {
            IOX.exception(ex);
        }
    }

    // Put it here in the code, not a classpath resource, to make it robust against repackaging (shading).
    public static String getDefaultString() {
        String defaultLog4j2 = """
                ## Command default log4j2 setup : log4j2 properties syntax.
                 status = error
                 name = DeltaLoggingDft

                 filters = threshold
                 filter.threshold.type = ThresholdFilter
                 filter.threshold.level = ALL

                 appender.console.type = Console
                 appender.console.name = OUT
                 appender.console.target = SYSTEM_OUT
                 appender.console.layout.type = PatternLayout
                 appender.console.layout.pattern = [%d{yyyy-MM-dd HH:mm:ss}] %-10c{1} %-5p %m%n

                 rootLogger.level                  = INFO
                 rootLogger.appenderRef.stdout.ref = OUT

                 ## Delta
                 logger.delta.name = Delta
                 logger.delta.level = INFO

                 logger.x0.name  = org.seaborne.delta
                 logger.x0.level = INFO

                 logger.x1.name  = Delta.HTTP
                 logger.x1.level = WARN

                 logger.x2.name  = org.apache.jena
                 logger.x2.level = WARN

                 logger.x3.name  = org.apache.jena.arq.info
                 logger.x3.level = INFO

                 logger.x4.name  = org.apache.jena.riot
                 logger.x4.level = INFO

                 logger.jetty.name = org.eclipse.jetty
                 logger.jetty.level = WARN

                 logger.zk.name = org.apache.zookeeper
                 logger.zk.level = WARN

                 logger.curator.name = org.apache.curator
                 logger.curator.level = WARN

                 ## logger.zk1.name = org.apache.zookeeper.server.ServerCnxnFactory
                 ## logger.zk1.level = ERROR

                 # Fuseki NCSA Request log.
                 appender.plain.type = Console
                 appender.plain.name = PLAIN
                 appender.plain.layout.type = PatternLayout
                 appender.plain.layout.pattern = %m%n

                 logger.fuseki-request.name                   = org.apache.jena.fuseki.Request
                 logger.fuseki-request.additivity             = false
                 logger.fuseki-request.level                  = OFF
                 logger.fuseki-request.appenderRef.plain.ref  = PLAIN
                 """ ;

        return defaultLog4j2;

    }
}
