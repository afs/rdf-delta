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

import static org.junit.Assert.assertEquals ;

import org.apache.jena.atlas.lib.FileOps ;
import org.junit.AfterClass ;
import org.junit.Before ;
import org.junit.Test ;
import org.seaborne.delta.PersistentCounter ;

public class TestPersistentCounter {
    static final String filename = "target/persistentCounter" ; 
    
    @Before public void before() {
        FileOps.deleteSilent(filename); 
    }
    
    @AfterClass public static void afterClass() {
        
    }

    @Test public void pCounter_01() {
        PersistentCounter pc = new PersistentCounter(filename) ;
        assertEquals(pc.get(), 0);
    }
    
    @Test public void pCounter_02() {
        PersistentCounter pc = new PersistentCounter(filename) ;
        pc.inc() ;
        assertEquals(pc.get(), 1);
        
        PersistentCounter pc2 = new PersistentCounter(filename) ;
        assertEquals(pc2.get(), 1);
    }
    
    @Test public void pCounter_03() {
        PersistentCounter pc = new PersistentCounter(filename) ;
        pc.set(10) ;
        assertEquals(10, pc.get());
        long x = pc.inc() ;
        assertEquals(x+1, pc.get());
        
        PersistentCounter pc2 = new PersistentCounter(filename) ;
        assertEquals(x+1, pc2.get());
    }


}
