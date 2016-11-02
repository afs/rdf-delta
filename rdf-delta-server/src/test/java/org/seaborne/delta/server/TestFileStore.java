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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.util.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileStore {
    static final String STORE = "target/test/store";
    static String regex = "target[\\\\/]test[/\\\\]store[/\\\\]FILE-([0-9]+)$";
    static Pattern pattern = Pattern.compile(regex);

    @BeforeClass
    static public void beforeClass() {
        FileOps.ensureDir(STORE);
    }

    @After
    public void afterTest() {
        FileStore.shutdown();
        FileOps.clearDirectory(STORE);
    }

    @Test
    public void fs_basic_01() {
        FileStore fs = FileStore.attach(STORE, "FILE");
        assertEquals(0, fs.getCurrentIndex());
        fs.basename(0);
        fs.filename(0);
    }

    @Test
    public void fs_basic_02() {
        FileStore fs = FileStore.attach(STORE, "FILE");
        assertEquals(0, fs.getCurrentIndex());
        Path p1 = fs.nextFilename().datafile;
        assertEquals(1, fs.getCurrentIndex());
        int idx1 = checkFilename(p1);
        assertEquals(1, idx1);
        
        Path p2 = fs.nextFilename().datafile;
        int idx2 = checkFilename(p2);
        assertEquals(2, fs.getCurrentIndex());
        assertEquals(2, idx2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fs_basic_03() {
        FileStore fs = FileStore.attach(STORE, "tmp");
    }

    @Test
    public void fs_write_01() throws IOException {
        FileStore fs = FileStore.attach(STORE, "FILE");
        assertEquals(0, fs.getCurrentIndex());
        FileEntry entry = fs.writeNewFile(out->{
            try(AWriter aw = IO .wrapUTF8(out)) {
              aw.write("abc");  
            } 
        }) ;
        assertNotNull(entry);
        assertNotNull(entry.datafile);
        int idx = checkFilename(entry.datafile);
        assertEquals(1, idx);
        // Read it back in again.
        String s = FileUtils.readWholeFileAsUTF8(entry.getDatafileName());
        assertEquals("abc", s);
    }
    
    private int checkFilename(Path path) {
        Matcher m = pattern.matcher(path.toString());
        assertTrue(m.find());
        String x = m.group(1);
        return Integer.parseInt(x);
    }

}
