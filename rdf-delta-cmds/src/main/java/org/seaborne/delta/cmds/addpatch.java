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

package org.seaborne.delta.cmds;

import java.io.IOException ;
import java.io.InputStream ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;

import jena.cmd.CmdException ;
import org.apache.jena.atlas.io.IO ;
import org.seaborne.delta.Id ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

/** Create a new log */
public class addpatch extends DeltaCmd {
    
    public static void main(String... args) {
        new addpatch(args).mainRun();
    }

    public addpatch(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL --dsrc NAME PATCH ...";
    }
    
    @Override
    protected void execCmd() {
        getPositional().forEach(fn->exec1(fn));
    }

    protected void exec1(String fn) {
        Path path = Paths.get(fn) ;
        
        Id dsRef = getDescription().getId();
        Id latest = dLink.getPatchLogInfo(dsRef).latestPatch;
        
        try(InputStream in = Files.newInputStream(path) ) {
            RDFPatch patch = RDFPatchOps.read(in);
            // Add previous.
            //patch.header().
            
            dLink.append(dsRef, patch);
        } catch (IOException ex ) { IO.exception(ex); }
    }
    
    @Override
    protected void checkForMandatoryArgs() {
        if ( !contains(argLogName) && ! contains(argDataSourceURI) ) 
            throw new CmdException("Required: one of --"+argLogName.getKeyName()+" or --"+argDataSourceURI.getKeyName());
        if ( getPositional().isEmpty() ) {
            throw new CmdException(getCommandName()+" : No patch files"); 
        }
    }
}
