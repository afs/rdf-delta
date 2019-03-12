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

package org.seaborne.delta.cmds;

import java.io.InputStream ;

import jena.cmd.CmdException;
import jena.cmd.CmdGeneral ;
import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesWriteUpdate;

/** Generate a SPARQL Update from a patches. */
public class patch2update extends CmdGeneral
{
    static { JenaSystem.init(); LogCtl.setCmdLogging() ; }

    public static void main(String... args) {
        new patch2update(args).mainRun();
    }

    public patch2update(String[] argv) {
        super(argv) ;
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" patch ...";
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();
    }

    @Override
    protected void exec() {
        AWriter out = IO.wrapUTF8(System.out);

        // Genralize to abstract class for any "apply" - patch2rdf
        RDFChanges c =  new RDFChangesWriteUpdate(out);

        // Patches
        // XXX
        if ( getPositional().isEmpty() )
            execOne(System.in);

        getPositional().forEach(fn->{
            // Check patch threading?
            RDFPatch patch = RDFPatchOps.read(fn);
            if ( isVerbose() ) {
                System.err.printf("# Patch id=%s", Id.str(patch.getId()));
                if ( patch.getPrevious() != null )
                    System.err.printf(" prev=%s", Id.str(patch.getPrevious()));
                System.err.println();
            }
            patch.apply(c);
        });
        out.flush();

    }

    private void apply(DatasetGraph dsg, RDFPatch patch) {
        RDFPatchOps.applyChange(dsg, patch);
    }

    private void execOne(InputStream input) {
        throw new CmdException("From InputStream (inc stdin) not yet supported");
    }

    @Override
    protected String getCommandName() {
        return "patch2rdf";
    }

}
