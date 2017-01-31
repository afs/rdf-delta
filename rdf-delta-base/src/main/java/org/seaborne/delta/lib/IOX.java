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
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.tdb.base.file.Location;

public class IOX {
    
    public static Path currentDirectory = Paths.get(".");
    
    public interface IOConsumer<X> {
        void actionEx(X arg) throws IOException;
    }
    
    /** Write a file safely - the change happens (the function returns true) or
     * somthing went wrong (the function throws a runtime exception) and the file is not changed.
     * Note that the tempfile must be in the same direct as the actual file so an OS-atomic rename can be done.  
     */
    public static boolean safeWrite(Path file, IOConsumer<OutputStream> writerAction) {
        Path tmp = createTempFile(file.getParent(), file.getFileName().toString(), ".tmp");
        return safeWrite(file, tmp, writerAction);
    }

    /** Write a file safely - the change happens (the function returns true) or
     * somthing went wrong (the function throws a runtime exception) and the file is not changed.
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
            FmtLog.error(IOX.class, ex, "IOException moving %s to %s", src, dst);
            IO.exception(ex);
        }
    }
    
    /** Copy a file, not atomic. *
     * Can copy to a directory or over an existing file.
     * @param srcFilename
     * @param dstFilename
     */
    public static void copy(String srcFilename, String dstFilename) {
        Path src = Paths.get(srcFilename);
        if ( ! Files.exists(src) )
            throw new RuntimeIOException("No such file: "+srcFilename);
        
        Path dst = Paths.get(dstFilename);
        if ( Files.isDirectory(dst) )
            dst = dst.resolve(src.getFileName());
        
        try { Files.copy(src, dst); }
        catch (IOException ex) {
            FmtLog.error(IOX.class, ex, "IOException copying %s to %s", srcFilename, dstFilename);
            IO.exception(ex);
        }
    }

    /** Convert a {@link Path}  to a {@link Location}. */
    public static Location asLocation(Path path) {
        if ( ! Files.isDirectory(path) )
            throw new RuntimeIOException("Path is not naming a directory: "+path);
        return Location.create(path.toString());
    }
    
    /** Convert a {@link Location} to a {@link Path}. */
    public static Path asPath(Location location) {
        if ( location.isMem() )
            throw new RuntimeIOException("Location is a memory location: "+location);
        return Paths.get(location.getDirectoryPath());
    }

    /** Read the whole of a file */
    public static byte[] readAll(Path pathname) {
        try {
            return Files.readAllBytes(pathname);
        } catch (IOException ex) {
            IO.exception(ex);
            return null;
        }
    }
    
    /** Write the whole of a file */
    public static void writeAll(Path pathname, byte[] value) {
        try {
            Files.write(pathname, value, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException ex) {
            IO.exception(ex);
        }
    }

    /**
     * Return a temporary filename path.
     * <p>
     * This operation is thread-safe.
     */
    public static Path createTempFile(Path dir, String prefix, String suffix, FileAttribute<? >... attrs) {
        // Java8 - Files.createTempFile - the temp file, when moved does not go away.
        try {
            return Files.createTempFile(dir, prefix, suffix, attrs);
        } catch (IOException ex) {
            IO.exception(ex); return null;
        }
        
        // --> IOX
        }
    
}
