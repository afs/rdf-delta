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

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.seaborne.delta.lib.IOX;

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

        // Stop Apache Jena command line logging initialization (Jena uses log4j1 for command line logging by default).
        System.setProperty("log4j.configuration", "off");

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
            ConfigurationSource source = new ConfigurationSource(input);
            ConfigurationFactory factory = ConfigurationFactory.getInstance();
            Configuration configuration = factory.getConfiguration(null, source);
            Configurator.initialize(configuration);
        }
        catch (IOException ex) {
            IOX.exception(ex);
        }
    }

    // Log4J2, non-strict XML format
    // Put it here in the code, not a classpath resource, to make it robust against repackaging (shading).
    public static String getDefaultString() {

        String defaultLog4j2_xml = String.join("\n"
            ,"<?xml version='1.0' encoding='UTF-8'?>"
            ,"<Configuration status='WARN'>"
            ,"  <Appenders>"
            ,"    <Console name='STDOUT' target='SYSTEM_OUT'>"
            ,"      <PatternLayout pattern='[%d{yyyy-MM-dd HH:mm:ss}] %-10c{1} %-5p %m%n'/>"
            ,"    </Console>"
            ,"    <Console name='PLAIN' target='SYSTEM_OUT'>"
            ,"      <PatternLayout pattern='%m%n' />"
            ,"    </Console>"
            ,"  </Appenders>"
            ,"  <Loggers>"
            ,"    <Root level='WARN'>"
            ,"      <AppenderRef ref='STDOUT'/>"
            ,"    </Root>"
            ,"    <Logger name='Delta' level='INFO'/>"
            ,"    <Logger name='org.seaborne.delta' level='INFO'/>"
            ,"    <Logger name='org.apache.jena' level='INFO'/>"
            // Built-in (co-resident) zookeeper
            ,"    <Logger name='org.apache.zookeeper' level='WARN'/>"
            // Eclispe Jetty - logs at startup at INFO
            ,"    <Logger name='org.eclipse.jetty'    level='WARN'/>"
            // Fuseki
            ,"    <Logger name='org.apache.jena.fuseki.Server' level='INFO'/>"
            ,"    <Logger name='org.apache.jena.fuseki.Fuseki' level='INFO'/>"
            // The Fuseki NCSA request log
            ,"    <Logger name='org.apache.jena.fuseki.Request'"
            ,"            additivity='false' level='OFF'>"
            ,"       <AppenderRef ref='PLAIN'/>"
            ,"    </Logger>"
            ,"  </Loggers>"
            ,"</Configuration>"
            );
        return defaultLog4j2_xml;
    }
}
