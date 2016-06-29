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

package org.seaborne.delta;

import java.util.concurrent.atomic.AtomicInteger ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class DPS {
    
    public static Logger LOG = LoggerFactory.getLogger("Server") ;
    public static Logger HTTP_LOG = LoggerFactory.getLogger("HTTP") ;
    
    static final String FILEBASE    = "Files" ;
    static final String BASEPATTERN = FILEBASE+"/patch-%04d" ;
    static final String TMPBASE     = FILEBASE+"/tmp-%04d" ;
    static AtomicInteger counter    = new AtomicInteger(10) ;
    
    static AtomicInteger tmpCounter = new AtomicInteger(0) ;
    
    public static String tmpFilename() {
        return String.format(TMPBASE, tmpCounter.incrementAndGet()) ;
    }
    
    public static String patchFilename(int idx) { 
        if ( idx < 0 )
            throw new IllegalArgumentException("idx = "+idx) ;
        return String.format(BASEPATTERN, idx) ;
    }
    
    public static String nextPatchFilename() {
        return patchFilename(counter.incrementAndGet()) ;
    }
}
