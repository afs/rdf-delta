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

import jena.cmd.CmdException ;
import org.seaborne.delta.DeltaNotFoundException ;
import org.seaborne.delta.Id ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

/** Create a new log */
public class getpatch extends DeltaCmd {
    
    public static void main(String... args) {
        new getpatch(args).mainRun();
    }

    public getpatch(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL --dsrc NAME id";
    }
    
    @Override
    protected void execCmd() {
        getPositional().forEach(this::exec1);
    }

    protected void exec1(String patchRef) {
        Id patchId = null;
        int patchVersion = -1;
        try {
            patchVersion = Integer.parseInt(patchRef);
        } catch (NumberFormatException ex) {
            throw new CmdException(getCommandName()+" : Invalid version");
        }
        
        Id dsRef =  getDescription().getId();

        RDFPatch patch;
        try {
            patch = dLink.fetch(dsRef, patchVersion);
            if ( patch == null )
                throw new CmdException(getCommandName()+" : No such patch : "+patchVersion);
            else
                RDFPatchOps.write(System.out, patch);
        } catch (DeltaNotFoundException ex) {
            // Bad dsRef.
            throw new CmdException(getCommandName()+" : No such patch : "+patchVersion);
        }
    }

    @Override
    protected void checkForMandatoryArgs() {
        if ( !contains(argLogName) && ! contains(argDataSourceURI) ) 
            throw new CmdException("Required: one of --"+argLogName.getKeyName()+" or --"+argDataSourceURI.getKeyName());
        if ( getPositional().isEmpty() ) {
            throw new CmdException(getCommandName()+" : No patch version"); 
        }
    }
}
