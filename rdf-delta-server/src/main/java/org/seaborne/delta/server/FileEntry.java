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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;

/**
 * Record of a file in a {@link FileStore}.
 * <p>
 * It provides the path to the file, a path to an associated temporary fileused to perform
 * atomic writes to the file.
 * <p>
 * {@code FileEntry} are "write once".
 * <p>
 * The write sequence is
 * <pre>
 *    FileStore fileStore = ...
 *    FileEntry entry = fileStore.allocateFilename();
 *    OutputStream out = entry.openForWrite();
 *    ... write contents ...
 *    entry.completeWrite();
 * </pre>
 */
public class FileEntry {
    public final int version;
    public final Path datafile;
    private final Path tmpfile;
    private OutputStream out = null ;
    private boolean haveWritten = false;
    
    /*package*/ FileEntry(int index, Path datafile, Path tmpfile) {
        this.version = index;
        this.datafile = datafile;
        this.tmpfile = tmpfile;
    }
    
    /**
     * Initiate the write process. The {@code Outstream} returned is to a
     * temporary file (same filing system) that is moved into place in 
     * {@link #completeWrite}, making the writing of the file atomic.
     * <p> 
     * Note that {@code Outstream.close} is idempotent - it is safe for the application to
     * close the {@code Outstream}.
     */
    public OutputStream openForWrite( ) {
        if ( haveWritten )
            throw new RuntimeIOException("FileEntry has already been written: "+datafile);
        try {
            return out = Files.newOutputStream(tmpfile) ;
        } catch (IOException ex) { IO.exception(ex); return null; } 
    }
    
    /**
     * Complete the write process: closes the OutputStream allocated by
     * {@link #openForWrite} thenn
     * 
     * <p>
     * Note that {@code Outstream.close} is idempotent - it is safe
     * for the application to close the {@code Outstream}.
     * <p>
     * The application must flush any buffered output prior to calling {@code completeWrite}.
     */
    public void completeWrite() {
        haveWritten = true;
        IO.close(out);
        out = null ;
        FileStore.move(tmpfile, datafile);
    }
    
    public String getDatafileName() {
        return datafile.toString();
    }
}