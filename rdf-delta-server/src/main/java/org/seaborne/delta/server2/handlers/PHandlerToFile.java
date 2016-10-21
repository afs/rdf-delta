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

package org.seaborne.delta.server2.handlers;

import java.io.* ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;

import org.apache.jena.atlas.io.IO ;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.lib.OutputStream2 ;
import org.seaborne.delta.server2.DPS ;
import org.seaborne.delta.server2.Patch ;
import org.seaborne.delta.server2.PatchHandler ;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.riot.tio.TokenWriter ;
import org.slf4j.Logger ;

/** Write a patch to a file.*/
public class PHandlerToFile implements PatchHandler {
    private static Logger LOG = DPS.LOG ;
    
    static private boolean verbose = false ;
    
    public PHandlerToFile() {}
    
    /** Safe handler */
    @Override
    synchronized // XXX revise
    public void handle(Patch patch) {
        String dst = DPS.nextPatchFilename() ;
        String s = DPS.tmpFilename() ;
        if ( verbose ) {
            LOG.info("<<<<-----------------") ;
            LOG.info("# Patch = "+dst+"("+s+")") ;
        }

        TokenWriter output = output(s) ;
        RDFChangesWriter scWriter = new RDFChangesWriter(output) ;
        patch.play(scWriter);
        move(s, dst) ; 
        if ( verbose )
            LOG.info(">>>>-----------------") ;
        else
            LOG.info("# Patch = "+dst) ;
    }


    // Must move a complete file into place
    private static void move(String src, String dst) {
        //System.err.printf("move %s to %s\n", src, dst) ;
        Path pSrc = Paths.get(src) ;
        Path pDst = Paths.get(dst) ;
        try { Files.move(pSrc, pDst) ; }
        catch (IOException ex) {
            LOG.warn(String.format("IOException moving %s to %s", src, dst) , ex);
            IO.exception(ex);
        }
    }

    static private TokenWriter output(String s) {
        OutputStream out = outputStream(s) ;
        return DeltaOps.tokenWriter(out) ;
    }

    static private OutputStream outputStream(String s) {
        Path p = Paths.get(s) ;
        if ( Files.exists(p) ) 
            System.out.println("Overwriting file"); 

        OutputStream out = null ;
        try { out = new FileOutputStream(s) ; }
        catch (FileNotFoundException e) {
            LOG.warn("File not found: {}", s) ;
            IO.exception(e);
        }
        out = new BufferedOutputStream(out) ;
        if ( ! verbose )
            return out ;
        // Copy to stdout.
        OutputStream out2 = new FilterOutputStream(System.out) { 
            @Override public void close() {}
        } ;

        return new OutputStream2(out2, out) ; 
    }
}
