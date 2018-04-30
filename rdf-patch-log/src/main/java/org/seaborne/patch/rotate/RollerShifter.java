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
import java.util.Comparator;
import java.util.regex.Pattern;

/** Roller where the files are "base" , "base.001", "base.002", ...
 * the current output file is always "base" and the files are 
 * shifted up on a rotate.    
 */
class RollerShifter implements Roller {
    private boolean valid = false;
    private final Path directory;
    private final String baseFilename;
    private final String filename;
    
    /** Match an incremental file (does not match the base file name). **/
    private static Pattern patternIncremental = FileMgr.patternIncremental;
    private static final String INC_SEP = FileMgr.INC_SEP;

    private static final String numFmt = "%d";
    private static final Comparator<Filename> cmpNumericModifier = FileMgr.cmpNumericModifier;

    
    RollerShifter(Path directory, String baseFilename, String format) {
        this.directory = directory;
        this.baseFilename = baseFilename;
        this.filename = directory.resolve(baseFilename).toString();
    }
    
    @Override
    public void startSection() {}

    @Override
    public void finishSection() {}
    
    @Override
    public void forceRollover() {
        valid = false;
    }
    
    @Override
    public boolean hasExpired() {
        return !valid;
    }
    
    @Override
    public String nextFilename() {
        valid = true;
        FileMgr.shiftFiles(directory, baseFilename, 1, "%03d");
        return filename; 
    }

}