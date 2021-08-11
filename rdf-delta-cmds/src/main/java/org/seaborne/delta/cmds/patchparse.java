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

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.sys.JenaSystem;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.PatchSummary;
import org.seaborne.patch.changes.RDFChangesCounter;

/** Parse patches as validation */
public class patchparse extends CmdPatch
{
    static {
        LogCtl.setLogging();
        JenaSystem.init();
    }

    public static void main(String... args) {
        new patchparse(args).mainRun();
    }

    public patchparse(String[] argv) {
        super(argv) ;
    }

    @Override
    protected String getCommandName() {
        return "patchparse";
    }

    @Override
    protected void execOne(String source, InputStream input) {
        RDFPatch patch = RDFPatchOps.read(input);
//        if ( patch.getId() == null )
//            System.err.printf("No patch source=%s\n", source);
        RDFPatchOps.write(System.out, patch);

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
}
