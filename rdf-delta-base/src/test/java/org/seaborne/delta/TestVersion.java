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

import static org.junit.Assert.*;

import org.apache.jena.atlas.json.JsonValue;
import org.junit.Test;

public class TestVersion {
    
    @Test public void version_1() {
        assertEquals(-1, Version.UNSET.value()); 
        assertEquals(0, Version.INIT.value());
        assertEquals(1, Version.FIRST.value());
    }

    @Test public void version_2() {
        Version ver = Version.FIRST.inc();
        assertEquals(2, ver.value());
    }
    
    private static Version rtJson(Version v) {
        JsonValue jv = v.asJson();
        Version ver2 = Version.create(jv);
        assertEquals(v, ver2);
        return ver2; 
    }

    /** Round trip one of the known versions */
    private static void rtConstJson(Version v) {
        Version ver2 = rtJson(v);
        assertSame(v, ver2);
    }

    @Test public void version_json_1() {
        Version ver = Version.create(9);
        rtJson(ver);
    }
    
    @Test public void version_json_2() {
        Version ver = Version.create(9);
        String s = ver.asParam();
        Version ver2 = Version.create(s);
        assertEquals(ver, ver2);
    }

    @Test public void version_json_3() {
        rtConstJson(Version.UNSET);
    }
    
    @Test public void version_json_4() {
        rtConstJson(Version.INIT);
    }
    
    @Test public void version_json_7() {
        Version v = rtJson(Version.FIRST);
        // Different object
        assertNotSame(Version.FIRST, v);
    }

    private static Version rtParam(Version v) {
        String s = v.asParam();
        Version ver2 = Version.create(s);
        assertEquals(v, ver2);
        return ver2; 
    }

    /** Round trip one of the known versions */
    private static void rtConstParam(Version v) {
        Version ver2 = rtParam(v);
        assertSame(v, ver2);
    }

    @Test public void version_param_1() {
        Version ver = Version.create(9);
        rtParam(ver);
    }
    
    @Test public void version_param_2() {
        Version ver = Version.create(9);
        String s = ver.asParam();
        Version ver2 = Version.create(s);
        assertEquals(ver, ver2);
    }

    @Test public void version_param_3() {
        rtConstParam(Version.UNSET);
    }
    
    @Test public void version_param_4() {
        rtConstParam(Version.INIT);
    }
    
    @Test public void version_param_7() {
        Version v = rtParam(Version.FIRST);
        // Different object
        assertNotSame(Version.FIRST, v);
    }

    @Test public void version_cmp() {
        Version ver9 = Version.create(9);
        Version ver8 = Version.create(8);
        
        assertTrue(ver8.isBefore(ver9));
        assertFalse(ver8.isAfter(ver9));
        
        assertFalse(ver9.isBefore(ver8));
        assertTrue(ver9.isAfter(ver8));
        
        assertFalse(ver8.isBefore(ver8));
        assertFalse(ver8.isAfter(ver8));
    }
}
