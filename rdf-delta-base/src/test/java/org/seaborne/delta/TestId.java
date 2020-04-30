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

package org.seaborne.delta;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.UUID;


public class TestId {

    @Test public void id_1() {
        Id id1 = Id.create();
        Id id2 = Id.create();
        assertNotEquals(id1, id2);
    }

    @Test public void id_2() {
        Id id1 = Id.nullId();
        Id id2 = Id.create();
        assertNotEquals(id1, id2);
    }

    @Test public void id_3() {
        Id id1 = Id.nullId();
        UUID uuid = UUID.fromString(id1.asPlainString());
        assertEquals(0L, uuid.getMostSignificantBits());
        assertEquals(0L, uuid.getLeastSignificantBits());
    }

    @Test public void id_fromString() {
        Id id1 = Id.create();
        assertTrue(id1.asNode().isURI());
        assertEquals(36, id1.asParam().length());
        Id id2 = Id.fromString(id1.asParam());
        assertEquals(id1, id2);
    }

    @Test public void id_fromNode() {
        Id id1 = Id.create();
        assertEquals(36, id1.asPlainString().length());
        Id id2 = Id.fromNode(id1.asNode());
        assertEquals(id1, id2);
    }

    @Test public void id_fromBytes() {
        Id id1 = Id.create();
        byte[] bytes = id1.asBytes();
        Id id2 = Id.fromBytes(bytes);
        assertNotNull(id2);
        assertNotSame(id1, id2);
        assertEquals(id1, id2);
    }

}
