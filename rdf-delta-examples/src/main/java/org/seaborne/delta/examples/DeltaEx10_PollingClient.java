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

package org.seaborne.delta.examples;

import java.util.Optional;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.logging.LogCtl;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.seaborne.delta.client.DataState;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.changes.RDFChangesWriter;
import org.seaborne.patch.text.TokenWriter;
import org.seaborne.patch.text.TokenWriterText;

/**
 * Example of a client that polls the log for changes. The client keeps a persistent
 * record of the last state that he client sync'ed to. When new patches appear, it prints
 * them out and updates the local persistent record.
 */
public class DeltaEx10_PollingClient {
    static { LogCtl.setJavaLogging(); }

    static final Object lock = new Object();

    public static void main(String[] args) {
        // Setup: find the patch log by name.
        // Make zone for the version tracking state.

        String LOG_NAME = "ABC";
        String ZONE_NAME = "ZoneTracker";
        if ( ! FileOps.exists(ZONE_NAME) ) {
            FileOps.ensureDir(ZONE_NAME);
        }
        //If you want to reset and start from the beginning of the log, delete the persistent state.
        // FileOps.clearAll(ZONE_NAME);

        DeltaLink dLink = DeltaLinkHTTP.connect("http://localhost:1066/");
        Zone zoneTracker = Zone.connect(ZONE_NAME);
        // Find the log details.
        Optional<PatchLogInfo> patchLog = dLink.listPatchLogInfo().stream()
            .filter(pInfo->LOG_NAME.equals(pInfo.getDataSourceName()))
            .findAny();
        if ( ! patchLog.isPresent() ) {
            System.err.println("** No patch log '"+ZONE_NAME+"' **");
            return;
        }
        // Set up the local zone tracking.
        PatchLogInfo info = patchLog.get();
        Id dsRef = patchLog.get().getDataSourceId();
        DataState dataState = zoneTracker.create(dsRef, info.getDataSourceName(), info.getDataSourceURI(), LocalStorageType.NONE);

        //Sync loop.
        for ( ;; ) {
            synchronized(lock) {
                // Update view of the remote log.
                info = patchLog.get();
                // Poll - get remote version, get any new patches.
                Version versionLocal = dataState.version();
                Version versionRemote = info.getMaxVersion();
                //System.out.printf("L:%s R:%s\n", versionLocal, versionRemote);
                if ( versionLocal.isBefore(versionRemote) ) {
                    // from versionLocal+1 to info.getMaxVersion();
                    for ( long version = versionLocal.value()+1; version <= versionRemote.value() ; version ++ ) {
                        RDFPatch patch = dLink.fetch(dsRef, Version.create(version));
                        System.out.println("Patch Version = "+version);
                        // ** Process patch **
                        // patch.apply(** application code implements RDFChanges**);

                        // Example: Print the patch.
                        // RDFPatchOps.write(System.out, patch1);
                        // which is:
                        TokenWriter tw = new TokenWriterText(System.out);
                        RDFChanges c = new RDFChangesWriter(tw);
                        patch.apply(c);
                        tw.flush();
                    }
                    // Move the local version forward.
                    dataState.updateState(versionRemote, dsRef);
                }
            }
            Lib.sleep(5_000);
        }
    }

}
