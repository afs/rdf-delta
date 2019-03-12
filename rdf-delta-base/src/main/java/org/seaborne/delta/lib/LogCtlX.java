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

package org.seaborne.delta.lib;

import java.io.InputStream;

import org.apache.jena.atlas.AtlasException;
import org.apache.jena.atlas.logging.LogCtl;

public class LogCtlX {
    
    // Expose in Jena LogCtl.
    public static boolean setJavaLoggingClasspath(String resourceName) {
        // Not "LogCtl.class.getResourceAsStream(resourceName)" which monkeys around with the resourceName.
        InputStream in = LogCtl.class.getClassLoader().getResourceAsStream(resourceName);
        if ( in != null ) {
            try {
                java.util.logging.LogManager.getLogManager().readConfiguration(in) ;
                return true; 
            } catch (Exception ex) {
                throw new AtlasException(ex) ;
            }
        }
        return false;
    }
}
