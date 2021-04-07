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

import org.apache.jena.cmd.CmdException ;
import org.seaborne.delta.*;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

/** GET a patch */
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
        if ( getPositional().isEmpty() ) {
            Id dsRef = getDescription().getId();
            PatchLogInfo logInfo = dLink.getPatchLogInfo(dsRef);
            if ( ! logInfo.getMaxVersion().isValid()) {
                throw new CmdException(getCommandName()+" : Empty log");
            }
            exec1(logInfo.getMaxVersion());
            return ;
        }

        getPositional().forEach(v->{
            long patchVersion;
            try {
                patchVersion = Integer.parseInt(v);
            } catch (NumberFormatException ex) {
                throw new CmdException(getCommandName()+" : Invalid version");
            }
            exec1(Version.create(patchVersion));
        });

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
