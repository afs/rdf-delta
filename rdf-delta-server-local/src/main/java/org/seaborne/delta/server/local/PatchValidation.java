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

import java.util.Objects;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DeltaBadPatchException;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchValidation {
    private static Logger LOG = LoggerFactory.getLogger(PatchValidation.class);
    
    /** Validate a patch for this {@code PatchLog} */
    public static boolean validate(RDFPatch patch, PatchLog log) {
        Id previousId = Id.fromNode(patch.getPrevious());
        Id patchId = Id.fromNode(patch.getId());
        if ( patchId == null ) {
            FmtLog.warn(LOG, "No patch id");
            return false;
        }
        
        if ( previousId == null ) {
            if ( !log.isEmpty() ) {
                FmtLog.warn(LOG, "No previous for patch when PatchLog is not empty: patch=%s", patchId);
                return false;
            }
        } else {
            if ( Objects.equals(patchId, previousId) ) {
                FmtLog.warn(LOG, "Patch ihas itself as the previous patch: patch=%s : previous=%s", patchId, previousId);
                return false ;
            }

            if ( log.isEmpty() ) {
                FmtLog.warn(LOG, "Previous reference for patch but PatchLog is empty: patch=%s : previous=%s", patchId, previousId);
                return false ;
            }
        }
        try {
            // XXX Why separate?
            validate(log, patch.header(), patchId, previousId, PatchValidation::badPatchEx);
            return true ;
        } catch (DeltaException ex) { return false; }
    }

    @FunctionalInterface public interface BadHandler { void bad(String fmt, Object ...args) ; }
    
    public static void validateNewPatch(PatchLog log, Id patchId, Id previousId, BadHandler action) {
        if ( patchId == null )
            action.bad("Patch: No id");
        if ( log.contains(patchId) )
            action.bad("Patch already exists: patch=%s", patchId);
        Id logHead = log.getLatestId();
        // Works if previousId == null.
        if ( ! Objects.equals(logHead, previousId) ) {
            action.bad("Previous not current: log head=%s : patch previous=%s",logHead, previousId);
        }
    }
    
    private static void validate(PatchLog log, PatchHeader header, Id patchId, Id previousId, BadHandler action) {
        if ( previousId != null ) {
            if ( ! log.contains(previousId) )
                action.bad("Patch previous not found: patch=%s, previous=%s", patchId, previousId);
            Node prevId = header.getPrevious() ;
            if ( ! previousId.asNode().equals(prevId) )
                action.bad("Patch previous header not found: patch=%s, previous=%s", patchId, previousId);
        } else {
            if ( header.getPrevious() != null )
                action.bad("Patch previous header not found: patch=%s, previous=%s", patchId, previousId);
        }
           
        if ( ! previousId.equals(log.getLatestId()) ) {
            // No empty log, previousId != null but does not match log head.
            // Validation should have caught this. 
            badPatchEx("Patch not an update on the latest logged one: id=%s prev=%s (log=[%d, %s])", 
                        patchId, previousId, log.getLatestVersion(), log.getLatestId());
        }
    }
    
    public static void badPatchEx(String fmt, Object...args) {
        badPatchWarning(fmt, args);
        String msg = String.format(fmt, args);
        throw new DeltaBadPatchException(msg);
    }
    
    public static void badPatchError(String fmt, Object...args) {
        FmtLog.error(LOG, String.format(fmt, args)); 
    }

    public static void badPatchWarning(String fmt, Object...args) {
        FmtLog.warn(LOG, String.format(fmt, args)); 
    }


}
