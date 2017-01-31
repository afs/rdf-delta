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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files ;
import java.nio.file.Path;
import java.nio.file.Paths ;
import java.util.Objects;

import org.seaborne.delta.lib.IOX;

/**
 * Persistent bytes, with some additional code to make
 * working with strings easier.
 * 
 * "State" is a number of bytes, assumed to be "small", (makes sense to read and write the whole state as one blob).  
 */
public class PersistentState {
    private Path pathname = null;
    private final Path dir ;
    
    private byte[] value = null;
    private String valueStr = null;
    
    public PersistentState(String filename) {
        Objects.requireNonNull(filename);
        this.pathname = Paths.get(filename).toAbsolutePath();
        this.dir = pathname.getParent();
        reset();
    }

    public void reset() {
        if ( Files.exists(pathname) ) {
            value = IOX.readAll(pathname) ;
            valueStr = null;
        } else {
            value = new byte[0];
            valueStr = null;
            writeLocation(); 
        }
    }

    
    public byte[] get() {
        return value ;
    }

    public String getString() {
        if ( valueStr == null )
            valueStr = new String(value,  StandardCharsets.UTF_8);
        return valueStr ;
    }

    public void set(byte[] x) {
        value = x ;
        valueStr = null;
        writeLocation();
    }

    public void setString(String str) {
        set(str.getBytes(StandardCharsets.UTF_8));
    }

    private void writeLocation() {
        IOX.safeWrite(pathname, out->out.write(value));
    }
}
