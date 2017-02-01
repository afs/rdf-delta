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

import static org.junit.Assert.assertEquals;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.atlas.lib.FileOps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestPersistentState {
    static final String filename = "target/persistentState"; 
    
    @Before public void before() {
        FileOps.deleteSilent(filename); 
    }
    
    @AfterClass public static void afterClass() {
        
    }

    @Test public void pState_01() {
        PersistentState ps = new PersistentState(filename);
        assertEquals(0, ps.get().length);
    }
    
    @Test public void pState_02() {
        PersistentState ps = new PersistentState(filename);
        byte[] b = new byte[4];
        Bytes.setInt(-99, b);
        ps.set(b);
        assertEquals(4, ps.get().length);
        PersistentState ps2 = new PersistentState(filename);
        assertEquals(4, ps2.get().length);
    }
    
    @Test public void pState_String_03() {
        PersistentState ps = new PersistentState(filename);
        String data = "test data\n\t";
        ps.setString(data);
        assertEquals(data, ps.getString());
        PersistentState ps2 = new PersistentState(filename);
        assertEquals(data, ps2.getString());
    }
    
    @Test public void pCounter_01() {
        PersistentState pc = new PersistentState(filename) ;
        assertEquals(0, pc.getInteger());
    }
    
    @Test public void pCounter_02() {
        PersistentState pc = new PersistentState(filename) ;
        pc.inc() ;
        assertEquals(1, pc.getInteger());
        
        PersistentState pc2 = new PersistentState(filename) ;
        assertEquals(1, pc2.getInteger());
    }
    
    @Test public void pCounter_03() {
        PersistentState pc = new PersistentState(filename) ;
        pc.setInteger(10) ;
        assertEquals(10, pc.getInteger());
        long x = pc.inc() ;
        assertEquals(11, pc.getInteger());
        
        PersistentState pc2 = new PersistentState(filename) ;
        assertEquals(11, pc2.getInteger());
    }

}
