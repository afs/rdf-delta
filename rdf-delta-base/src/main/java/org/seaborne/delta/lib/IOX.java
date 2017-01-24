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

package org.seaborne.delta.lib;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.FmtLog;

public class IOX {
    
    public static Path currentDirectory = Paths.get(".");
    
    public interface IOConsumer<X> {
        void actionEx(X arg) throws IOException;
    }
    
    /** Write a file safely - the change happens (the function returns true) or
     * somthign went wrong (the function throws a runtime exception) and the file is not changed.
     * Note that the tempfile must be in the same direct as the actual file so an OS-atomic rename can be done.  
     */
    public static boolean safeWrite(Path file, Path tmpFile, IOConsumer<OutputStream> writerAction) {
        try {
            try(OutputStream out = new BufferedOutputStream(Files.newOutputStream(tmpFile)) ) {
                writerAction.actionEx(out);
            }
            move(tmpFile, file);
        } catch(IOException ex) { IO.exception(ex); /*Not reached*/return false; }
        return true;
    }

    /** Atomically move a file. */
    public static void move(Path src, Path dst) {
        try { Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE) ; }
        catch (IOException ex) {
            FmtLog.warn(IOX.class, ex, "IOException moving %s to %s", src, dst);
            IO.exception(ex);
        }
    }
}
