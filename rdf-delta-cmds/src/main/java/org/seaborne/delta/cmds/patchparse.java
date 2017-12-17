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

import java.io.IOException;
import java.io.InputStream ;

import jena.cmd.CmdGeneral ;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.JenaSystem;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.PatchSummary;
import org.seaborne.patch.changes.RDFChangesCounter;

/** Parse patches as validation. */
public class patchparse extends CmdGeneral
{
    static { JenaSystem.init(); LogCtl.setCmdLogging() ; }
    
    public static void main(String... args) {
        new patchparse(args).mainRun();
    }

    public patchparse(String[] argv) {
        super(argv) ;
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" FILE..." ;
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();
    }
    
    @Override
    protected void exec() {
        // Patches
        if ( getPositional().isEmpty() )
            execOne("Stdin", System.in);
        else {
            getPositional().forEach(fn->{
                try ( InputStream in = IO.openFile(fn) ) {
                    execOne(fn, in);
                } catch (IOException ex) { IO.exception(ex); return; }
            });
        }
    }
    
    private void apply(DatasetGraph dsg, RDFPatch patch) {
        RDFPatchOps.applyChange(dsg, patch);
    }

    private void execOne(String source, InputStream input) {
        //System.err.println("Source = "+source);
        RDFPatch patch = RDFPatchOps.read(input);
//        if ( patch.getId() == null )
//            System.err.printf("No patch source=%s\n", source);
        
        //RDFChanges changes = RDFPatchOps.changesPrinter();
        RDFChanges changes = RDFPatchOps.changesOut();
        patch.apply(changes);
        
        if ( isVerbose() ) {
            System.err.printf("# Patch id=%s", Id.str(patch.getId()));
            if ( patch.getPrevious() != null )
                System.err.printf(" prev=%s", Id.str(patch.getPrevious()));
            System.err.println();
        }
        RDFChangesCounter counter = new RDFChangesCounter();
        patch.apply(counter);
        PatchSummary summary = counter.summary();
        if ( summary.countTxnCommit == 0 && summary.countTxnAbort == 0 )
            System.err.printf("# No commit source=%s, id=%s\n", source, Id.str(patch.getId()));
    }
    
    @Override
    protected String getCommandName() {
        return "patchparse";
    }
    
}
