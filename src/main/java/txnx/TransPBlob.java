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

import org.apache.jena.atlas.io.IO ;

/** A MR+SW transactional 'thing' */ 
public abstract class TransPBlob<X> extends TransactionalBlob<X> {
    private final Path file ;
    private final Path jrnl ;
    private byte[] serialized ;
    
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
    
    protected abstract X getUninitalized() ;

    protected abstract byte[] toBytes(X x) ;
    protected abstract X fromBytes(byte[] bytes) ;
    
    private X initialize() {
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
    /** Finish committing  in a write transaction */  
    @Override
    protected void commitFinish() {
        write(file, serialized) ;
        serialized = null ;
        try { 
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

    
    private static void write(Path fn, byte[] b) {
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(fn.toFile()), 64*1024)) {
            out.write(b) ;
        } catch (IOException ex) {
            IO.exception(ex);
        }
    }
    
    private static byte[] read(Path fn) {
        try (InputStream in = new BufferedInputStream(new FileInputStream(fn.toFile()), 64*1024)) { 
            return IO.readWholeFile(in) ;
        } catch (FileNotFoundException ex) {
            return null ;
        } catch (IOException ex) {
            IO.exception(ex); return null ;
        }
    }
}
