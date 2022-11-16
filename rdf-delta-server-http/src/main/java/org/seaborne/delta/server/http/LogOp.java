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

package org.seaborne.delta.server.http;

import java.io.IOException ;
import java.io.OutputStream ;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonBuilder ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.WebContent ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC ;
import org.eclipse.jetty.io.RuntimeIOException;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaBadPatchException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.DeltaNotFoundException;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.apache.jena.rdfpatch.PatchException;
import org.apache.jena.rdfpatch.RDFPatch ;
import org.apache.jena.rdfpatch.RDFPatchOps ;
import org.slf4j.Logger ;

/** Patch Log operations */
public class LogOp {
    static private Logger LOG = Delta.getDeltaLogger("Patch") ;

    /** Execute an append, assuming the action has been verified that it is an append operation */
    public static void append(DeltaAction action) throws IOException {
        Id dsRef = idForDatasource(action);
        if ( dsRef == null )
            throw new DeltaNotFoundException("No such datasource: '"+action.httpArgs.datasourceName+"'");

        RDFPatch patch;
        try {
            patch = readPatch(action);
        } catch (IOException ex) {
            FmtLog.error(LOG, ex, "[%d] Patch:append ds:%s patch:failed: %s", action.id, dsRef.toString(), ex.getMessage());
            throw new RuntimeIOException(ex);
        } catch (PatchException ex) {
            FmtLog.warn(LOG, ex, "[%d] Patch:append ds:%s patch:syntax error: %s", action.id, dsRef.toString(), ex.getMessage());
            throw ex;
        }

        Node patchId = patch.getId();
        if ( false )
            RDFPatchOps.write(System.out, patch);

        try {
            Version version = action.dLink.append(dsRef, patch);

            // Location of patch in "container/patch/id" form.
            //String location = action.request.getRequestURI()+"/patch/"+ref.asPlainString();
            String location = action.request.getRequestURI()+"?version="+version;

            JsonValue rslt = JsonBuilder.create()
                .startObject()
                .pair(DeltaConst.F_VERSION, version.asJson())
                .pair(DeltaConst.F_LOCATION, location)
                .finishObject()
                .build();

            FmtLog.info(LOG, "[%d] Patch:append ds:%s patch:%s => ver=%s", action.id, dsRef.toString(), Id.str(patchId), version);

            OutputStream out = action.response.getOutputStream();
            action.response.setContentType(WebContent.contentTypeJSON);
            action.response.setStatus(HttpSC.OK_200);
            action.response.setHeader(HttpNames.hLocation, location);

            JSON.write(out, rslt);
            out.flush();
        } catch (DeltaBadPatchException ex) {
            FmtLog.warn(LOG, /*ex,*/ "[%d] Patch:append ds:%s patch:%s => %s", action.id, dsRef.toString(), Id.str(patchId), ex.getMessage());
            throw ex;
        } catch (IOException ex) {
            FmtLog.error(LOG, ex, "[%d] Patch:append ds:%s patch:%s => %s", action.id, dsRef.toString(), Id.str(patchId), ex.getMessage());
            throw ex;
        }
    }

    private static RDFPatch readPatch(DeltaAction action) throws IOException {
        HttpServletRequest request = action.request;
        long byteLength = request.getContentLengthLong();
        try ( CountingInputStream in = new CountingInputStream(request.getInputStream()); ) {
            RDFPatch patch = RDFPatchOps.read(in);
            if ( byteLength != -1L ) {
                if ( in.getByteCount() != byteLength )
                    FmtLog.warn(LOG, "[%d] Length mismatch: Read: %d : Content-Length: %d", action.id, in.getByteCount(),  byteLength);
            }
            return patch;
        }
    }

    private static Id idForDatasource(DeltaAction action) {
        String datasourceName = action.httpArgs.datasourceName;
        if ( Id.maybeUUID(datasourceName) ) {
            // Looks like an Id
            try {
                UUID uuid = UUID.fromString(datasourceName);
                Id id = Id.fromUUID(uuid);
                DataSourceDescription dsd = action.dLink.getDataSourceDescription(id);
                return dsd != null ? id : null;
            } catch (IllegalArgumentException ex) { /* Not a UUID: drop through to try-by-name */ }
        }
        // Not a UUID.
        DataSourceDescription dsd = action.dLink.getDataSourceDescriptionByName(datasourceName);
        return dsd != null ? dsd.getId() : null;
    }

    public static void fetch(DeltaAction action) throws IOException {
        Id dsRef = idForDatasource(action);
        if ( dsRef == null )
            throw new DeltaNotFoundException("No such datasource: '"+action.httpArgs.datasourceName+"'");
        RDFPatch patch;

        if ( action.httpArgs.patchId != null ) {
            Id patchId = action.httpArgs.patchId;
            FmtLog.info(LOG, "[%d] Patch:fetch Dest=%s, Patch=%s", action.id, dsRef, patchId);
            patch = action.dLink.fetch(dsRef, patchId);
            if ( patch == null )
                throw new DeltaNotFoundException("Patch not found: id="+patchId);
        } else if ( action.httpArgs.version != null ) {
            Version ver = Version.create(action.httpArgs.version);
            FmtLog.info(LOG, "[%d] Patch:fetch Dest=%s, Patch=%s", action.id, dsRef, ver);
            patch = action.dLink.fetch(dsRef, ver);
            if ( patch == null )
                throw new DeltaNotFoundException("Patch not found: version="+action.httpArgs.version);
        } else {
            DeltaAction.errorBadRequest("No id and no version in patch fetch request");
            patch = null;
        }

        OutputStream out = action.response.getOutputStream();
        //action.response.setCharacterEncoding(WebContent.charsetUTF8);
        action.response.setStatus(HttpSC.OK_200);
        action.response.setContentType(DeltaConst.contentTypePatchText);
        RDFPatchOps.write(out, patch);
        // Not "close".
        IO.flush(out);
    }
}
