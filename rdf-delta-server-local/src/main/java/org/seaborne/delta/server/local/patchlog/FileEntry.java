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

package org.seaborne.delta.server.local.patchlog;

import java.io.IOException ;
import java.io.OutputStream;
import java.nio.file.Files ;
import java.nio.file.Path;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.IOX.IOConsumer;

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
    public final long version;
    public final Path datafile;
    private final Path tmpfile;
    
    // Only for openForWrite / completeWrite
    private OutputStream out = null ;
    private boolean haveWritten = false;
    
    /*package*/ FileEntry(long index, Path datafile, Path tmpfile) {
        this.version = index;
        this.datafile = datafile;
        this.tmpfile = tmpfile;
    }
    
    /** Atomiclly write the file */
    public void write(IOConsumer<OutputStream> action) {
        if ( haveWritten )
            throw new RuntimeIOException("FileEntry has already been written: "+datafile);
        IOX.safeWrite(datafile, tmpfile, action);
        haveWritten = true;
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
        } catch (IOException ex) { throw IOX.exception(ex); }
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
        IO.close(out);
        IOX.move(tmpfile, datafile);
        haveWritten = true;
        out = null ;
    }
    
    public String getDatafileName() {
        return datafile.toString();
    }
    
    @Override
    public String toString() {
        // Dirctory is quite long.
        return String.format("FileEntry[%d, %s, %s]", version, 
                             datafile.getFileName(), tmpfile.getFileName(), datafile.getParent());
    }
}