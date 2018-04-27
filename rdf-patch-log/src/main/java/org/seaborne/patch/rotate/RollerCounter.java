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

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Roller where the files are "0001", "0002", "0003", The files are not moved ; the next
 * index in sequence is the next filename. See {@link RollerShifter} for shifting all the
 * file names up and having a fixed current filename.
 */
class RollerCounter implements Roller {
    // Explicit rollover.
    
    private final Path directory;
    private final String baseFilename;
    private final String indexFormat;

//  /** Match an incremental file (does not match the base file name). **/
//  private static Pattern patternIncremental = Pattern.compile("(.*)(\\.)(\\d+)");
//  private static final String INC_SEP = ".";
//
//  private static String numFmt = "%d";
    public static Comparator<Filename> cmpNumericModifier = FileMgr.cmpNumericModifier;

    
    private final Pattern patternFilename = Pattern.compile("(.*)(-)(\\d)*");
    private final String  fmtModifer = "%04d";
    private static final String INC_SEP = FileMgr.INC_SEP;
    
    private Long currentId = null;
    private boolean valid = false;
    
    RollerCounter(Path directory, String baseFilename, String indexFormat) {
        this.directory = directory;
        this.baseFilename = baseFilename;
        this.indexFormat = indexFormat;
        init(directory,baseFilename);
    }

    private void init(Path directory, String baseFilename) {
        List<Filename> filenames = FileMgr.scan(directory, baseFilename, patternFilename);
        if ( ! filenames.isEmpty() ) {
            Filename max = Collections.max(filenames, cmpNumericModifier.reversed());
            currentId = Long.parseLong(max.modifier);
        }
        else
            currentId = 0L ;
    }
    
    @Override
    public void startSection() {}


    @Override
    public void finishSection() {
        // Each section is in its own files
        //valid = false;
    }
    
    @Override
    public void forceRollover() {
        valid = false;
    }
    
    @Override
    public boolean hasExpired() {
        return !valid;
    }
    
    private long nextIndex() {
        return currentId+1;
    }
    
    
    @Override
    public String nextFilename() {
        valid = true;
        long idx = nextIndex();
        String fn = FileMgr.freshFilename(directory, baseFilename, (int)idx, INC_SEP, fmtModifer);
        return fn; 
    }
}