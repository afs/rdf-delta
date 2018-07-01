/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.server.local;

import static org.seaborne.delta.DeltaConst.F_LOG_TYPE ;
import static org.seaborne.delta.DeltaConst.F_VERSION;
import static org.seaborne.delta.DeltaConst.SYSTEM_VERSION ;

import java.io.IOException ;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects ;
import java.util.Properties;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.io.IndentedWriter ;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;

public class LocalServerConfig {
    
    public static LocalServerConfig basic() { return LocalServerConfig.create().build(); }  

    /** Name of the default PatchStore provider */ 
    private final String logProvider;
    
    /** File name of the configuration file (if any - may be null) */
    private final String configFile;
    
    /** Delta system properties */
    private final Properties properties; 

    private LocalServerConfig(String logProvider, Properties properties, String configFile) {
        this.logProvider = logProvider;
        this.configFile = configFile;
        this.properties = properties; 
    }
    
    /** Name of the default PatchStore provider */
    public String getLogProvider() {
        return logProvider ;
    }

    /** File name of the config file (if any - may be null) */
    public String getConfigFile() {
        return configFile ;
    }

    /** Get property */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    private static void handleMissingConfigFile(Path path) {
        //throw new DeltaConfigException("No such file: "+path.toString());
        JsonObject obj = JSONX.buildObject(b->{
            b.key(F_VERSION).value(DeltaConst.SYSTEM_VERSION);
            // Default log provider
            b.key(F_LOG_TYPE).value(DeltaConst.LOG_FILE);
        });
        try ( IndentedWriter out = new IndentedWriter(Files.newOutputStream(path)); ) {
            JSON.write(out, obj);
            out.ensureStartOfLine();
        } catch (IOException ex) { IO.exception(ex); }
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

    /** Configuration builder.
     *  Call "parse" or ("setPort" and "setLocation").
     *  Can call "parse" and then modify 
     */
    static public class Builder {
        private static Logger LOG = Delta.DELTA_CONFIG_LOG;
        private String configFile = null;
        private String logProvider = null;
        private final Properties properties = new Properties(); 
    
        
        public Builder setLogProvider(String logProvider) {
            Objects.requireNonNull(logProvider);
            this.logProvider = logProvider;
            return this;
        }
        
        public Builder setProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }
        
        public Builder setProperties(Properties properties) {
            properties.forEach((k,v)->setProperty((String)k, (String)v));
            return this;
        }
    
        /** Parse a configuration file. */
        public Builder parse(String configFile) {
            Path path = Paths.get(configFile);
            if ( ! Files.exists(path) )
                handleMissingConfigFile(path);
            
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
            logProvider = DPS.PatchStoreFileProvider;
            String logTypeName = JSONX.getStrOrNull(obj, F_LOG_TYPE);
            if ( logTypeName != null ) {
                String providerName = PatchStoreMgr.canonical(logTypeName);
                if ( providerName == null )
                    providerName = DeltaConst.LOG_FILE;
                logProvider = providerName;
            }
            setProperty("delta.file", path.getParent().toString());
            return this;
        }
        
        public LocalServerConfig build() {
            String provider = null;
            if ( logProvider != null )
                provider = PatchStoreMgr.canonical(logProvider);
            return new LocalServerConfig(provider, properties, configFile);
        }
    }
    
}
