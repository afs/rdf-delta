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

package org.seaborne.delta.server;

import java.io.File ;
import java.util.concurrent.atomic.AtomicInteger ;
import java.util.regex.Matcher ;
import java.util.regex.Pattern ;

import org.seaborne.delta.Delta ;
import org.slf4j.Logger ;

public class DPS {
    
    public static Logger LOG = Delta.DELTA_LOG ;
    public static Logger HTTP_LOG = Delta.DELTA_HTTP_LOG ;
    
    /* Patch file counter.
     *  This is the index of the highest used number.
     *  File naming usually begins at 0001.   
     */
    public static final AtomicInteger counter = new AtomicInteger(0) ;
    
    // XXX [Delta] Safety and synchronization.
    // -> have a "system state object" that gets replaced by reset. 
    
    public static int getPatchIndex() {
        return counter.get();
    }
    
    static final AtomicInteger tmpCounter = new AtomicInteger(0) ;
    
    /** Find the highest index in a directpry of files */
    public static int scanForIndex(String directory, String namebase) {
        // TODO Come back and make efficient??
        // - e.g. a PersistentCounter and scan up from there only
        // - or no scan and combinew with the safe-write dance. 
        Pattern pattern = Pattern.compile(namebase+"([0-9]*)") ;
        int max = -1 ;
        String[] x = new File(directory).list() ;
        if ( x == null )
            // No directory.
            return -1 ;
        for ( String fn : x ) {
            Matcher m = pattern.matcher(fn) ;
            if ( ! m.matches() ) {              // anchored
                LOG.info("No match: "+fn) ;
                continue ;
            }
            LOG.info("Match:    "+fn) ;
            String numStr = m.group(1) ;
            int num = Integer.parseInt(numStr) ;
            max = Math.max(max, num) ;
        }
        return max ;
    }
    
    private static volatile boolean initialized = false ; 
    public static void init() { 
        if ( initialized ) 
            return ;
        synchronized(DPS.class) {
            if ( initialized ) 
                return ;
            initialized = true ;
            initOnce() ;
        }
    }
    
    private static void initOnce() {
    }
}
