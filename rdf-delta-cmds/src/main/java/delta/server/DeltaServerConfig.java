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

package delta.server;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.io.IOX;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.server.Provider;

/**
 * DeltaServer configuration.
 */
public class DeltaServerConfig {
    // Server level.
    // Either a port number xor a Jetty configuration file.
    // If there is a jetty.xml filename, the port is ignored.
    public Integer serverPort = null;
    public String jettyConf   = null;

    // Provider. Assumes necessary classes are on the classpath.
    public Provider provider = Provider.UNSET;

    // File provider
    public String fileBase = null ;

    // ---- JSON field constants
    private static String fProvider           = "store";

    private static String fPort               = "port";
    private static String fJetty              = "jetty";

    // The File provider
    private static String fFileDirData        = "filestore";
    // The memory provider
    // -- none

    // ---- JSON field constants

    public DeltaServerConfig( ) {}

    public static DeltaServerConfig read(String file) {
        JsonObject obj = JSON.read(file);
        return create(obj);
    }

    public static DeltaServerConfig read(InputStream input) {
        JsonObject obj = JSON.parse(input);
        return create(obj);
    }

    private static void validate(DeltaServerConfig conf) {}

    /** Recreate a {@code DeltaServerConfig} from JSON */
    public static DeltaServerConfig create(JsonObject obj) {
        DeltaServerConfig conf = new DeltaServerConfig();
        if ( obj.hasKey(fPort) ) {
            int x = JSONX.getInt(obj, fPort, -1);
            if ( x >= 0 )
                conf.serverPort = x;
        }

        if ( obj.hasKey(fProvider) )
            conf.provider = Provider.create(JSONX.getStrOrNull(obj, fProvider));

        // Local on-disk file area.
        if ( obj.hasKey(fFileDirData) )
            conf.fileBase = JSONX.getStrOrNull(obj, fFileDirData);

        // Custom Jetty server configuration.
        if ( obj.hasKey(fJetty) )
            conf.jettyConf = JSONX.getStrOrNull(obj, fJetty);

        validate(conf);
        return conf;
    }

    public JsonObject asJSON() {
        return
            JSONX.buildObject(b->{
                if ( provider != null && provider != Provider.UNSET )
                    b.pair(fProvider, provider.name());

                if ( serverPort != null )
                    b.pair(fPort, serverPort.intValue());

                if ( jettyConf != null )
                    b.pair(fJetty, jettyConf);

                if ( fileBase != null )
                    b.pair(fFileDirData, fileBase);
            });
    }

    public static void writeJSON(DeltaServerConfig config, String file) {
        IOX.run(()->{
            JsonObject obj = config.asJSON();
            try (OutputStream out = Files.newOutputStream(Paths.get(file))) {
                JSON.write(out, obj);
        }});
    }

    public static void writeJSON(DeltaServerConfig config, OutputStream out) {
        JsonObject obj = config.asJSON();
        JSON.write(out, obj);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileBase == null) ? 0 : fileBase.hashCode());
        result = prime * result + ((jettyConf == null) ? 0 : jettyConf.hashCode());
        result = prime * result + ((provider == null) ? 0 : provider.hashCode());
        result = prime * result + ((serverPort == null) ? 0 : serverPort.hashCode());
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
        DeltaServerConfig other = (DeltaServerConfig)obj;
        if ( fileBase == null ) {
            if ( other.fileBase != null )
                return false;
        } else if ( !fileBase.equals(other.fileBase) )
            return false;
        if ( jettyConf == null ) {
            if ( other.jettyConf != null )
                return false;
        } else if ( !jettyConf.equals(other.jettyConf) )
            return false;
        if ( provider != other.provider )
            return false;
        if ( serverPort == null ) {
            if ( other.serverPort != null )
                return false;
        } else if ( !serverPort.equals(other.serverPort) )
            return false;
        return true;
    }
}
