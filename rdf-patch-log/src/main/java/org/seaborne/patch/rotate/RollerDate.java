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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.jena.atlas.logging.FmtLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filename policy where files are "filebase-yyyy-mm-dd" */  
class RollerDate implements Roller {
    private static final Logger LOG = LoggerFactory.getLogger(RollerDate.class);
    // Date-based roll over
    private final Path directory;
    private final String baseFilename;
    private LocalDate current = LocalDate.now();
    
    /** Match a date-appended filename */ 
    private static final Pattern regexDate = Pattern.compile("(.*)(-)(\\d{4}-\\d{2}-\\d{2})");
    private static final String DATE_SEP = "-";
    private static final DateTimeFormatter fmtDate = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final Comparator<Filename> cmpDate = (x,y)->{
        LocalDate xdt = filenameToDate(x);
        LocalDate ydt = filenameToDate(y);
        return xdt.compareTo(ydt); 
    };
    
    private static LocalDate filenameToDate(Filename filename) {
        return LocalDate.parse(filename.modifier, fmtDate);
    }
    
    RollerDate(String directoryName, String baseFilename) {
        this.directory = Paths.get(directoryName);
        this.baseFilename = baseFilename;
        init(directory,baseFilename);
    }
    
    private void init(Path directory, String baseFilename) {
        List<Filename> filenames = FileMgr.scan(directory, baseFilename, regexDate);
        if ( ! filenames.isEmpty() ) {
            LocalDate dateLast = filenameToDate(Collections.max(filenames, cmpDate));
            LocalDate dateFirst = filenameToDate(Collections.min(filenames, cmpDate));
            int problems = 0 ;
            if ( dateLast.isAfter(current)) {
                problems++;
                FmtLog.warn(LOG, "Latest output file is dated after today: %s > %s", dateLast, current);
            }
            if ( dateFirst.isAfter(current)) {
                problems++;
                FmtLog.warn(LOG, "First output file is dated after today: %s > %s", dateFirst, current);
            }
            if ( problems > 0 )
                throw new FileRotateException("Existing files dated into the future"); 
        }
    }
    
    @Override
    public boolean hasExpired() {
        return ( LocalDate.now().isAfter(current) ) ;
    }
    
    @Override
    public void forceRollover() {
        // No-op.
    }
    
    @Override
    public String nextFilename() {
        LocalDate nextCurrent = LocalDate.now();
        String fn = baseFilename + DATE_SEP + nextCurrent.format(fmtDate);
        current = nextCurrent;
        Path path = directory.resolve(fn);
        if ( Files.exists(path) )
            FmtLog.warn(LOG, "Using existing file: "+fn); 
        return fn.toString();
    }
}