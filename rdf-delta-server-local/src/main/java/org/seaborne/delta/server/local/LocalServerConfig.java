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

import static org.seaborne.delta.DPConst.F_PORT;
import static org.seaborne.delta.DPConst.F_VERSION;
import static org.seaborne.delta.DPConst.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;
import org.slf4j.Logger;

public class LocalServerConfig {
    /** Location of server area for Datasources */ 
    public Location location;
    /** Port number */
    public int port;
    /** File name of the config file (if any - may be null) */
    public String configFile;
    
    private LocalServerConfig(Location location, int port, String configFile) {
        this.location = location;
        this.port = port;
        this.configFile = configFile;
    }
    
    public static Builder create() { return new Builder(); }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((configFile == null) ? 0 : configFile.hashCode());
        result = prime * result + ((location == null) ? 0 : location.hashCode());
        result = prime * result + port;
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
        if ( port != other.port )
            return false;
        return true;
    }

    /** Configuration builder.
     *  Call "parse" or ("setPort" and "setLocation").
     *  Can call "parse" and then modify 
     */
    static public class Builder {
        private static Logger LOG = Delta.DELTA_CONFIG_LOG;
        private Location location = null;
        private int port = DPConst.PORT;
        private String configFile = null;
        
        public Builder setPort(int port) {
            this.port = port ;
            return this;
        }
        
        public Builder setLocation(Location location) {
            if ( ! location.exists() )
                throw new DeltaConfigException("No such location: "+location);
            this.location = location ;
            return this;
        }

        public Builder setLocation(String location) {
            Location loc = Location.create(location);
            return setLocation(loc);
        }
        
        /** parse a config file.
         * If Location is not set, default the location to the directory of the configuration file. 
         */
        public Builder parse(String configFile) {
            Path path = Paths.get(configFile);
            if ( ! Files.exists(path) )
                throw new DeltaConfigException("No such file: "+configFile);
            if ( location == null ) {
                Path locPath = path.getParent();
                location = IOX.asLocation(locPath);
            }

            JsonObject obj = JSON.read(configFile);
            int version = JSONX.getInt(obj, F_VERSION, -99);
            if ( version == -99 ) {
                LOG.warn("No version number for the configuration file : assuming 'current'");
                version = DPConst.SYSTEM_VERSION;
            }
            if ( version != SYSTEM_VERSION )
                throw new DeltaConfigException("Version number for LocalServer must be "+DPConst.SYSTEM_VERSION+".");
            
            int port = JSONX.getInt(obj, F_PORT, -1);
            if ( port <= 0  ) {
                LOG.info("No port number: Using default "+PORT);
                port = PORT;
            }
            setPort(port);
            this.configFile = configFile;
            return this;  
        }
        
        public LocalServerConfig build() {
            return new LocalServerConfig(location, port, configFile);
        }
    }
    
}
