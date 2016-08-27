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

package txnx;

import java.io.* ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;
import java.util.Map ;
import java.util.concurrent.ConcurrentHashMap ;

import org.apache.jena.atlas.io.IO ;
import org.seaborne.delta.lib.L ;

/** A MR+SW transactional 'thing' */ 
public abstract class TransPBlob<X> extends TransactionalBlob<X> {
    private final Path file ;
    private final Path jrnl ;
    private byte[] serialized ;
    // Don't really read/write bytes. Development mode.
    private boolean tempMode = true ;
    static private Map<Path, byte[]> files = new ConcurrentHashMap<>() ;
    
    protected TransPBlob(String mainFile, String jrnlFile) {
        super(null) ;
        this.file = Paths.get(mainFile) ;
        this.jrnl = Paths.get(jrnlFile) ;
        X x = initialize() ;
        if ( x == null ) {
            x = getUninitalized() ;
            writeState(file, x) ; 
        }
        super.setInitial(x);
    }

    /** Get first value */
    protected abstract X getUninitalized() ;

    /** Convert from X to bytes for the peristent storage */ 
    protected abstract byte[] toBytes(X x) ;
    /** Convert from bytes from the peristent storage to X */ 
    protected abstract X fromBytes(byte[] bytes) ;
    
    /** Start from the journal file, or the data file, or return null. */ 
    private X initialize() {
        if ( tempMode )
            return null ;
        
        if ( Files.exists(jrnl) ) {
            byte[] b = read(jrnl) ;
            write(file, b) ;
            return fromBytes(b) ;
        }
        if ( Files.exists(file) ) {
            byte[] b = read(file) ;
            return fromBytes(b) ;
        }
        return null ;
    }
    
    public String getFilename() {
        return file.getFileName().toString() ;
    }
    
    /** Prepare for commit in a write transaction - includes writing state to disk */ 
    @Override
    protected void commitPrepare() {
        // Cached
        serialized = toBytes(super.getDataState()) ;
        write(jrnl, serialized) ; 
    }
    
    /** Finish committing in a write transaction */  
    @Override
    protected void commitFinish() {
        write(file, serialized) ;
        serialized = null ;
        try { 
            if ( ! tempMode )
                Files.deleteIfExists(jrnl) ;
        } catch (IOException ex) {
            IO.exception(ex);
        }
    }
    
    // XXX Revisit. See TDB2 transactional blobs.
    
    private void writeState(Path fn, X x) {
        write(fn, toBytes(x)) ;
    }

    private X readState(Path fn) {
        byte b[] = read(fn) ;
        return fromBytes(b) ;
    }
    
    private void write(Path fn, byte[] b) {
        if ( tempMode )
            writeMem(fn, b) ;
        else
            writePersistent(fn, b) ;
    }
    
    private byte[] read(Path fn) {
        if ( tempMode )
            return readMem(fn) ;
        else
            return readPersistent(fn) ;
    }
    
    private static byte[] readPersistent(Path fn) {
        try (InputStream in = new BufferedInputStream(new FileInputStream(fn.toFile()), 64*1024)) { 
            return IO.readWholeFile(in) ;
        } catch (FileNotFoundException ex) {
            return null ;
        } catch (IOException ex) {
            IO.exception(ex); return null ;
        }
    }
    private static void writePersistent(Path fn, byte[] b) {
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(fn.toFile()), 64*1024)) {
            out.write(b) ;
        } catch (IOException ex) {
            IO.exception(ex);
        }
    }
    
    // Very careful - copy-in, copy-out.
    
    private static byte[] readMem(Path fn) {
        byte[] b = files.get(fn) ;
        return L.copy(b) ;
    }

    private static void writeMem(Path fn, byte[] b) {
        byte[] b1 = L.copy(b) ;
        files.put(fn, b1) ;
    }
    

}
