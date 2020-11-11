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

package org.seaborne.delta.fuseki.cmd;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.fuseki.main.cmds.FusekiMainCmd;
import org.apache.jena.fuseki.system.FusekiLogging;

public class DeltaFusekiServerCmd {

    public static void main(String[] args) {
        // Stop Fuseki trying to initialize logging using log4j.
        System.setProperty("log4j.configuration", "delta");
        // In case, we are invoked directly, not via dcmd.
        String[] log4j2files = { "log4j2.properties", "log4j2.yaml", "log4j2.yml", "log4j2.json", "log4j2.jsn", "log4j2.xml" };
        for ( String fn : log4j2files ) {
            if ( FileOps.exists(fn) ) {
                // Let Log4j2 initialize normally.
                System.setProperty("log4j.configurationFile", fn);
                break;
            }
        }
        FusekiLogging.markInitialized(true);
        FusekiMainCmd.main(args);
    }
}
