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

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.io.IndentedWriter ;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.local.patchlog.PatchStoreMgr ;
import org.slf4j.Logger;

public class LocalServerConfig {
    /** Location of server area for DataSources */ 
    private final Location location;
    
    /** Name of the default PatchStore provider */ 
    private final String logProvider;
    
    /** File name of the config file (if any - may be null) */
    private final String configFile;
    
    private LocalServerConfig(Location location, String logProvider, String configFile) {
        this.location = location;
        this.logProvider = logProvider;
        this.configFile = configFile;
    }
    
    /** Location of server area for DataSources */ 
    public Location getLocation() {
        return location ;
    }

    /** Name of the default PatchStore provider */ 
    public String getLogProvider() {
        return logProvider ;
    }

    /** File name of the config file (if any - may be null) */
    public String getConfigFile() {
        return configFile ;
    }

    public static Builder create() { return new Builder(); }
    
    /** Configuration builder.
     *  Call "parse" or ("setPort" and "setLocation").
     *  Can call "parse" and then modify 
     */
    static public class Builder {
        private static Logger LOG = Delta.DELTA_CONFIG_LOG;
        private Location location = null;
        private String configFile = null;
        private String logProvider = null;
        
        public Builder setLocation(Location location) {
            Objects.requireNonNull(location);
            if ( ! location.exists() )
                throw new DeltaConfigException("No such location: "+location);
            this.location = location ;
            return this;
        }

        public Builder setLocation(String location) {
            Objects.requireNonNull(location);
            Location loc = Location.create(location);
            return setLocation(loc);
        }
        
        public Builder setLogProvider(String logProvider) {
            Objects.requireNonNull(logProvider);
            this.logProvider = logProvider;
            return this;
        }
        
        /** parse a config file.
         * If Location is not set, default the location to the directory of the configuration file. 
         */
        public Builder parse(String configFile) {
            Path path = Paths.get(configFile);
            if ( ! Files.exists(path) )
                handleMissingConfigFile(path);
            
            if ( location == null ) {
                Path locPath = path.getParent();
                location = IOX.asLocation(locPath);
            }

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
            String logTypeName = JSONX.getStrOrNull(obj, F_LOG_TYPE);
            String providerName = PatchStoreMgr.shortName2LongName(logTypeName);
            
            if ( providerName == null )
                providerName = DeltaConst.LOG_FILE;
            logProvider = providerName;
            return this;  
        }
        
        public LocalServerConfig build() {
            return new LocalServerConfig(location, logProvider, configFile);
        }
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
        result = prime * result + ((location == null) ? 0 : location.hashCode());
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
        if ( location == null ) {
            if ( other.location != null )
                return false;
        } else if ( !location.equals(other.location) )
            return false;
        return true;
    }
    
}
