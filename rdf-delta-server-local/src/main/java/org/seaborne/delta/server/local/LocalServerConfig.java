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

package org.seaborne.delta.server.local;

import static org.seaborne.delta.DeltaConst.F_LOG_TYPE;
import static org.seaborne.delta.DeltaConst.F_STORE;
import static org.seaborne.delta.DeltaConst.F_VERSION;
import static org.seaborne.delta.DeltaConst.SYSTEM_VERSION ;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects ;
import java.util.Properties;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.Provider;
import org.slf4j.Logger;

public class LocalServerConfig {

    //public static LocalServerConfig basic() { return LocalServerConfig.create().build(); }

    /** Name of the default PatchStore provider for new patch logs.*/
    private final Provider logProvider;

    /** File name of the configuration file (if any - may be null) */
    private final String configFile;

    /** Delta properties */
    private final Properties properties;

    private final String jettyConf;

    private LocalServerConfig(Provider logProvider, Properties properties, String configFile, String jettyConf) {
        Objects.requireNonNull(logProvider);
        this.logProvider = logProvider;
        this.configFile = configFile;
        this.properties = properties;
        this.jettyConf = jettyConf;
    }

    /** Name of the default PatchStore provider */
    public Provider getLogProviderType() {
        return logProvider ;
    }

    /** File name of the config file (if any - may be null) */
    public String getConfigFile() {
        return configFile ;
    }

    /** Get property, return null for no found. */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    /** Get the Jetty server configuration file, if any. */
    public String getJettyConfigFile() {
        return jettyConf;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((configFile == null) ? 0 : configFile.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        LocalServerConfig other = (LocalServerConfig)obj;
        if ( configFile == null ) {
            if ( other.configFile != null )
                return false;
        } else if ( !configFile.equals(other.configFile) )
            return false;
        return true;
    }

    public static Builder create() { return new Builder(); }

    //public static Builder create(Provider logProvider) { return new Builder().setLogProvider(logProvider); }

    public static Builder create(LocalServerConfig other) { return new Builder(other); }

    /** Configuration builder.
     *  Call "parse" or ("setPort" and "setLocation").
     *  Can call "parse" and then modify
     */
    static public class Builder {
        private static Logger LOG = Delta.DELTA_CONFIG_LOG;
        private String configFile = null;
        private Provider logProvider = null;
        private String jettyConfigFile = null;
        private final Properties properties = new Properties();

        public Builder() {}

        public Builder(LocalServerConfig other) {
            this.configFile = other.configFile;
            this.logProvider = other.logProvider;
            copyPropertiesInto(other.properties, this.properties);
        }

        public Builder setLogProvider(Provider logProvider) {
            Objects.requireNonNull(logProvider);
            this.logProvider = logProvider;
            return this;
        }

        public Builder setProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        public Builder setProperties(Properties properties) {
            copyPropertiesInto(properties, this.properties);
            return this;
        }

        public Builder jettyConfigFile(String jettyConfigFile) {
            this.jettyConfigFile  = jettyConfigFile;
            return this;
        }


        /** Copy properties from {@code src} to {@code dest}. */
        private static void copyPropertiesInto(Properties src, Properties dest) {
            src.forEach((k,v)->dest.setProperty((String)k, (String)v));
        }

        /** Parse a configuration file. */
        public Builder parse(String configFile) {
            // Relationship to DeltaServerConfig in command line.
            Path path = Paths.get(configFile);
            if ( ! Files.exists(path) )
                throw new DeltaConfigException("File not found: "+configFile);
            // -- version
            JsonObject obj = JSON.read(configFile);
            int version = JSONX.getInt(obj, F_VERSION, -99);
            if ( version == -99 ) {
                LOG.warn("No version number for the configuration file : assuming 'current'");
                version = DeltaConst.SYSTEM_VERSION;
            }
            if ( version != SYSTEM_VERSION )
                throw new DeltaConfigException("Version number for LocalServer must be "+DeltaConst.SYSTEM_VERSION+".");

            this.configFile = configFile;

            // -- log provider
            // Default.
            logProvider = Provider.LOCAL;
            String logTypeName = JSONX.getStrOrNull(obj, F_LOG_TYPE);
            if ( logTypeName != null ) {
                Provider provider = DPS.providerByName(logTypeName);
                if ( provider == null )
                    throw new DeltaConfigException("Unknown log type: "+logTypeName);
                logProvider = provider ;
            }
            // -- store (file, rocks, any local)
            if ( isLocalProvider(logProvider) ) {
                String store = JSONX.getStrOrNull(obj, F_STORE);
                Path storeLocation = null;
                if ( store == null )
                    // Default to directory where the config file is.
                    storeLocation = path.getParent();
                else
                    storeLocation = path.getParent().resolve(store);

                if ( storeLocation != null )
                    setProperty(DeltaConst.pDeltaStore, storeLocation.toString());
            }
            // TODO -- General properties.
            return this;
        }

        private static boolean isLocalProvider(Provider provider) {
            switch(provider) {
                case LOCAL: case FILE: case ROCKS: return true;
                default: return false;
            }
        }

        public LocalServerConfig build() {
            return new LocalServerConfig(logProvider, properties, configFile, jettyConfigFile);
        }
    }

}
