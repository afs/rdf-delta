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

package org.seaborne.patch.rotate;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Semaphore;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.FmtLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputStreamMgr {
    private static Logger LOG = LoggerFactory.getLogger(OutputStreamMgr.class); 
    
    // The file area 
    private final Path directory;
    private final String filebase;
    // Current active file
    private String currentFilename = null; 
    
    // One writer at a time.
    private Semaphore sema = new Semaphore(1);
    // The output
    private FileOutputStream fileOutput = null;
    // Buffered output stream used by the caller.
    private OutputStream output = null;
    
    static DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;

    // File Policy
    private final Roller roller; 

    
    // Number of writes this process-lifetime.
    private long counter = 0;

    public OutputStreamMgr(String directoryName, String baseFilename, FilePolicy strategy) {
        this.directory = Paths.get(directoryName);
        this.filebase = baseFilename;
        //this.roller = new RollerDate(directoryName, baseFilename);
        //this.roller = new RollerCounter(directoryName, baseFilename);
        this.roller = roller(directoryName, baseFilename, strategy);
    }
    
    private static Roller roller(String directoryName, String baseFilename, FilePolicy strategy) {
        switch ( strategy ) {
            case DATE :         return new RollerDate(directoryName, baseFilename);
            case INDEX :        return new RollerCounter(directoryName, baseFilename, "%04d");
            case SHIFT :        return new RollerShifter(directoryName, baseFilename, "%03d");
            case TIMESTAMP :    return new RollerTimestamp(directoryName, baseFilename);
            case FIXED :         return new RollerFixed(directoryName, baseFilename);
        }
        return null;
    }
    
    public OutputStream output() {
        try {
            sema.acquire();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        advanceIfNecessary();
        return new OutputStreamPooled(this, output);
    }
    
    /** Force a rotation of the output file. */
    public void rotate() {
        roller.forceRollover();
    }

    /*package*/ void finishOutput() {
        try {
            // Flush the BufferedOutputStream to the FileOutputStream
            output.flush();
            // fsync the FileOutputStream to storage
            fileOutput.getFD().sync();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        sema.release();
    }

    private boolean hasActiveFile() { 
        return output != null;
    }
        
    private void advanceIfNecessary() {
        // Inside ownership of the semaphore.
        // Other rules
        if ( roller.hasExpired() )
            closeOutput();
        if ( ! hasActiveFile() )
            nextFile();
    }

    private void flushOutput() {
        try {
            output.flush();
            fileOutput.getFD().sync();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void closeOutput() {
        if ( output == null )
            return;
        flushOutput();
        IO.close(output);
        output = null;
        fileOutput = null;
        currentFilename = null;
    }

    private void nextFile() {
        try {
            currentFilename = roller.nextFilename();
            FmtLog.info(LOG, "Setup: %s", currentFilename);
            // Must be a FileOutputStream so that getFD().sync is available.
            fileOutput = new FileOutputStream(currentFilename, true);
            output = new BufferedOutputStream(fileOutput);
        } catch (FileNotFoundException ex) {
            IO.exception(ex);
            return;
//        } catch (IOException ex) {
//            IO.exception(ex);
//            return;
        }
    }
}
