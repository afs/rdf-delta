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

package org.seaborne.delta.pubsub;

import java.io.InputStream ;
import java.io.OutputStream ;
import java.util.ArrayList ;
import java.util.List ;

import org.seaborne.delta.server.FileEntry;
import org.seaborne.delta.server.FileStore;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.changes.RDFChangesLog ;
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
        addProcessor(new RDFChangesLog(RDFChangesLog::printer)) ;
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
    
    public synchronized FileEntry receive(InputStream in, RDFChanges changes) {
        FileEntry entry = fileStore.allocateFilename();
        RDFChangesWriter dest = destination(entry) ;
        RDFChanges pipeline = dest ;
        // Insert the extra pipeline stage.
        if ( changes != null )
            pipeline = RDFChangesN.multi(changes, pipeline) ;
        // --- Additional processors called before making safe.
        for ( RDFChanges p : additionalProcessors )
            pipeline = RDFChangesN.multi(p, pipeline) ;
        
//        // If last is abort ...
//        // Add a "last capture"
//        // If all are abort, loose it.
//        
//        RDFChanges abortOrCommit = new RDFChangesBase() {
//            int commitCount = 0 ;
//            int abortCount = 0 ;
//            // And is it last?
//            
//            @Override
//            public void txnCommit() { commitCount++ ; }
//            
//            @Override
//            public void txnAbort() { abortCount++ ; }
//        } ;
        
        PatchReader scr = new PatchReader(in) ;
        pipeline.start();
        scr.apply(pipeline);
        pipeline.finish() ;
        dest.flush();
        entry.completeWrite();
        // Now safe on disk.
        return entry;
    }
}
