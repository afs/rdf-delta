/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta;

import java.io.IOException ;
import java.io.OutputStream ;
import java.io.OutputStreamWriter ;
import java.io.Writer ;
import java.nio.charset.StandardCharsets ;
import java.nio.file.Files ;
import java.nio.file.Paths ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.atlas.logging.Log ;
import org.seaborne.delta.lib.IOX;

/** A persistent integer value */
public class PersistentCounter {
    private final String filename ;
    private final String dir ;
    private final Object lock = new Object(); 
    
    private long value = 0 ;
    
    public PersistentCounter(String filename) {
        this.filename = filename ;
        String x = FileOps.basename(filename)  ;
        if ( x == null || x.isEmpty() )
            x = "." ;
        this.dir = x ;
        
        if ( Files.exists(Paths.get(filename)) ) {
            value = read(filename) ;
            return ;
        }
         
        value = 0 ;
        writeLocation(); 
    }
    
    public long get() {
        return value ;
    }

    // i++
    public long inc() {
        synchronized(lock) {
            long x = (value++) ;
            set(value) ;
            return x ;
        }
    }

    public void set(long x) {
        synchronized(lock) {
            value = x ;
            writeLocation();
        }
    }

    private void readLocation() {
        if ( filename != null ) {
            if ( ! FileOps.exists(filename) ) {
                set(0);
                return ;
            }
            long x = read(filename) ;
            value = x ;
        }
    }

    private void writeLocation() {
        writeLocation(value) ;
    }
    
    private void writeLocation(long value) {
        if ( filename != null ) {
            write(filename, value) ;
        }
    }

    //-- Read/write the value
    // This should really be checksum'ed or other internal check to make sure IO worked.  
    private static long read(String filename) {
        try {
            String str = IO.readWholeFileAsUTF8(filename) ;
            if ( str.endsWith("\n") ) {
                str = str.substring(0, str.length()-1) ;
            }
            str = str.trim() ;
            return Long.parseLong(str) ;
        } 
        catch (IOException ex) {
            Log.error(PersistentCounter.class, "IOException: " + ex.getMessage(), ex) ;
            throw IOX.exception(ex);
        }
        catch (NumberFormatException ex) {
            Log.error(PersistentCounter.class, "NumberformatException: " + ex.getMessage()) ;
            throw new InternalErrorException(ex) ;
        }
    }
    
    private static void write(String filename, long value) {
        try { writeStringAsUTF8(filename, Long.toString(value)) ; } 
        catch (IOException ex) {}
        catch (NumberFormatException ex) {}
    }
    
    // ==> IO.writeWholeFileAsUTF8
    
    /** Write a string to a file as UTF-8. The file is closed after the operation.
     * @param filename
     * @param content String to be writtem
     * @throws IOException
     */
    
    private /*public*/ static void writeStringAsUTF8(String filename, String content) throws IOException {
        try ( OutputStream out = IO.openOutputFileEx(filename) ) {
            writeStringAsUTF8(out, content) ;
            out.flush() ;
        }
    }

    /** Read a whole stream as UTF-8
     * 
     * @param out       OutputStream to be read
     * @param content   String to be written
     * @throws  IOException
     */
    private /*public*/ static void writeStringAsUTF8(OutputStream out, String content) throws IOException {
        Writer w = new OutputStreamWriter(out, StandardCharsets.UTF_8) ;
        w.write(content);
        w.flush();
        // Not close.
    }

    // ==> IO.writeWholeFileAsUTF8
    
}
