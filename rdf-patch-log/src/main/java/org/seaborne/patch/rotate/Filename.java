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

import org.apache.jena.atlas.logging.FmtLog;

public class Filename {
    public final Path directory;
    public final String basename;
    public final String separator;
    public final String modifier;
    public Filename(Path directory, String basename, String separator, String modifier) {
        this.directory = directory;
        this.basename = basename;
        
        if ( 1 == countNonNulls(separator, modifier) ) {
            FmtLog.warn(FileMgr.LOG, "Both separator and modifier must be set, or both be null: (s=%s, m=%s)", separator, modifier);
            separator = null;
            modifier = null;
        }
        
        this.separator = separator;
        this.modifier = modifier;
    }
    
    public boolean isBasename() {
        // 
        return modifier==null || separator==null;
    }
    
    /** As a filename, without directory. */
    public String asFilenameString() {
        if ( isBasename() )
            return basename;
        return basename+separator+modifier;
    }
    
    // display version.
    @Override
    public String toString() {
        if ( isBasename() )
            return directory+"|"+basename;
        return directory+"|"+basename+"|"+separator+"|"+modifier;
    }
    
    private static int countNonNulls(Object ... objects) {
        int x = 0;
        for ( Object obj : objects ) {
            if ( obj != null )
                x++;
        }
        return x;
    }
}