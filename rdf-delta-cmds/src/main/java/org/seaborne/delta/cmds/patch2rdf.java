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

import arq.cmdline.ModDatasetAssembler;
import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.CmdException;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.Txn;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

/** Apply patches to a base RDF file (or empty dataset). */
public class patch2rdf extends CmdGeneral
{
    static {
        LogCtl.setLogging();
        JenaSystem.init();
    }

    protected ModDatasetAssembler modDataset  = new ModDatasetAssembler();
    protected ArgDecl dataDecl                = new ArgDecl(ArgDecl.HasValue, "data");

    public static void main(String... args) {
        new patch2rdf(args).mainRun();
    }

    public patch2rdf(String[] argv) {
        super(argv);
        super.addModule(modDataset);
        super.add(dataDecl);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+"[--data QUADS | --desc ASSEMBLER ] FILE...";
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();
        if ( modDataset.getAssemblerFile() != null && super.hasArg(dataDecl) )
            throw new CmdException("Both an assembler file and a data file specified");
    }

    @Override
    protected void exec() {
        DatasetGraph dsg;

        boolean writeOnFinish = false;

        // Data.
        if ( modDataset.getAssemblerFile() != null )
            dsg = modDataset.getDatasetGraph();
        else {
            dsg = DatasetGraphFactory.createTxnMem();
            if ( super.hasArg(dataDecl) ) {
                getValues(dataDecl).forEach(fn->{
                    Txn.executeWrite(dsg, ()->RDFDataMgr.read(dsg, fn));
                });
            }
            writeOnFinish = true;
        }

        // Patches
        if ( getPositional().isEmpty() ) {
            RDFPatch patch = RDFPatchOps.read(System.in);
            apply(dsg, patch);
        }

        getPositional().forEach(fn->{
            // Check patch threading?
            RDFPatch patch = RDFPatchOps.read(fn);
            if ( isVerbose() ) {
                System.err.printf("# Patch id=%s", Id.str(patch.getId()));
                if ( patch.getPrevious() != null )
                    System.err.printf(" prev=%s", Id.str(patch.getPrevious()));
                System.err.println();
            }
            apply(dsg, patch);
        });

        if ( writeOnFinish )
            Txn.executeRead(dsg, ()->RDFDataMgr.write(System.out, dsg, Lang.TRIG));
    }

    private void apply(DatasetGraph dsg, RDFPatch patch) {
        RDFPatchOps.applyChange(dsg, patch);
    }

    @Override
    protected String getCommandName() {
        return "patch2rdf";
    }
}
