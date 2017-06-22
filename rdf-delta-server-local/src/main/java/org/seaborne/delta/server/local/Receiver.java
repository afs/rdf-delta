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

package org.seaborne.delta.server.local;

import java.io.InputStream ;
import java.io.OutputStream ;
import java.util.ArrayList ;
import java.util.List ;
import java.util.function.Consumer ;

import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesLogSummary;
import org.seaborne.patch.changes.RDFChangesN ;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.riot.tio.TokenWriter ;
import org.seaborne.riot.tio.impl.TokenWriterText ;

public class Receiver {
    private FileStore fileStore;
    private List<RDFChanges> additionalProcessors = new ArrayList<>();
    /*
     * In-bound processing: parse (=check) and place in the patch area
     */
    public Receiver(FileStore fileStore) {
        this.fileStore = fileStore;
        addProcessor(new RDFChangesLogSummary(DeltaOps.printerToLog(Delta.DELTA_PATCH))) ;
    }
    
    // -- Builderish.
    
    public void addProcessor(RDFChanges changes) {
        additionalProcessors.add(changes) ;
    }

    private RDFChangesWriter destination(FileEntry entry) {
        OutputStream out = entry.openForWrite();
        TokenWriter tw = new TokenWriterText(out) ;
        RDFChangesWriter dest = new RDFChangesWriter(tw) ;
        return dest ;
    }
    
    public FileEntry receive(RDFPatch patch, RDFChanges changes) {
        return receiveWorker((pipeline) -> patch.apply(pipeline), changes) ;
    }
    
    public FileEntry receive(InputStream in, RDFChanges changes) {
        PatchReader scr = new PatchReader(in) ;
        return receiveWorker((pipeline) ->scr.apply(pipeline), changes) ;
    }
    
    private synchronized FileEntry receiveWorker(Consumer<RDFChanges> processor, RDFChanges changes) {
        FileEntry entry = fileStore.allocateFilename();
        RDFChangesWriter dest = destination(entry) ;
        
        // Set up other processing.
        RDFChanges pipeline = dest ;
        // Insert the extra pipeline stage.
        if ( changes != null )
            pipeline = RDFChangesN.multi(changes, pipeline) ;
        // --- Additional processors called before making safe.
        for ( RDFChanges p : additionalProcessors )
            pipeline = RDFChangesN.multi(p, pipeline) ;
        
        // Pipeline
        pipeline.start();
        processor.accept(pipeline);
        pipeline.finish();
        
        // Finish up.
        dest.flush();
        dest.close();
        entry.completeWrite();
        fileStore.completeWrite(entry);
        return entry;
    }
}
