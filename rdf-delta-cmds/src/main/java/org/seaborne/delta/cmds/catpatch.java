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

import static java.lang.String.format;

import java.util.Collections;

import org.apache.jena.cmd.CmdException;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DeltaNotFoundException;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.apache.jena.rdfpatch.PatchHeader;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch ;
import org.apache.jena.rdfpatch.RDFPatchOps ;
import org.apache.jena.rdfpatch.changes.RDFChangesExternalTxn;
import org.apache.jena.rdfpatch.text.RDFChangesWriterText;

/** Output all the patches of a log as a single patch. */
public class catpatch extends DeltaCmd {

    public static void main(String... args) {
        new catpatch(args).mainRun();
    }

    public catpatch(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL --dsrc NAME";
    }

    @Override
    protected void execCmd() {
        Id dsRef = getDescription().getId();
        PatchLogInfo logInfo = dLink.getPatchLogInfo(dsRef);

        PatchHeader noHeader = new PatchHeader(Collections.emptyMap());

        try ( RDFChangesWriterText cw = RDFPatchOps.textWriter(System.out) ) {
            // Strip transactions.
            RDFChanges c = new RDFChangesExternalTxn(cw);

            Version minVer = logInfo.getMinVersion();
            Version maxVer = logInfo.getMaxVersion();
            if ( ! minVer.isValid() ) {
                System.out.println("# No patches");
                return;
            }
            if ( ! maxVer.isValid() ) {
                System.out.printf("# Bad log: minVer=%s maxVer=%s\n", minVer, maxVer);
                return;
            }

            // Outer TX - TC.
            cw.txnBegin();
            // Meta data.

            // Fetch max version for the id.
            RDFPatch patchMax = fetchPatch(dsRef, maxVer);
            Node catLatest = patchMax.getId();
            cw.header(RDFPatch.PREVIOUS, catLatest);

            // All but last.
            for ( long ver = minVer.value() ; ver < maxVer.value() ; ver++ ) {
                Version verObj = Version.create(ver);
                RDFPatch patch = fetchPatch(dsRef, verObj);
                // No header.
                RDFPatchOps.withHeader(noHeader, patch).apply(c);
            }
            // Last.
            RDFPatchOps.withHeader(noHeader, patchMax).apply(c);
            cw.txnCommit();
        }
        return;
    }

    private RDFPatch fetchPatch(Id dsRef, Version version) {
        try {
            RDFPatch patch = dLink.fetch(dsRef, version);
            if ( patch == null ) {
                throw new CmdException(format("Play: %s patch=%s : not found\n", dsRef, version));
            }
            return patch;
        } catch (DeltaNotFoundException ex) {
            // Which ever way it is signalled.  This way means "bad datasourceId"
            throw new CmdException(format("Play: %s patch=%s : not found (no datasource)\n", dsRef, version));
        }
    }

    protected void exec1(Version patchVersion) {
        Id patchId = null;

        Id dsRef = getDescription().getId();

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
    }
}
