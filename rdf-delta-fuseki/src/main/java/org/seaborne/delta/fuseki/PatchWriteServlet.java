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

package org.seaborne.delta.fuseki;

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.contentTypePatchBinary;
import static org.seaborne.delta.DeltaConst.ctPatchBinary;
import static org.seaborne.delta.DeltaConst.ctPatchText;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import javax.servlet.http.HttpServlet;

import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.server.CounterName;
import org.apache.jena.fuseki.servlets.*;
import org.apache.jena.fuseki.system.ActionCategory;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.web.HttpSC;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.filelog.FilePolicy;
import org.seaborne.patch.filelog.OutputMgr;
import org.seaborne.patch.filelog.rotate.ManagedOutput;

/**
 * A patch receiver. This {@link HttpServlet servlet} writes patches to a log file
 * according to a {@link FilePolicy} and so it functions as a backup server
 * <p>
 * It is a Fuseki servlet and not a dataset service.
 */
public class PatchWriteServlet extends ServletProcessor {
    static CounterName counterPatches     = CounterName.register("RDFpatch-write", "rdf-patch.write.requests");
    static CounterName counterPatchesGood = CounterName.register("RDFpatch-write", "rdf-patch.write.good");
    static CounterName counterPatchesBad  = CounterName.register("RDFpatch-write", "rdf-patch.write.bad");
    private ManagedOutput output;

    public PatchWriteServlet(String dir, String fn, FilePolicy policy) {
        super(Fuseki.actionLog, ActionCategory.ACTION);
        this.output = OutputMgr.create(Paths.get(dir), fn , policy);
    }

    public PatchWriteServlet(String filename, FilePolicy policy) {
        super(Fuseki.actionLog, ActionCategory.ACTION);
        this.output = OutputMgr.create(filename, policy);
    }

    // ---- POST or OPTIONS

    @Override
    public void execPost(HttpAction action) {
        this.validate(action);
        this.operation(action);
    }

    @Override
    public void execOptions(HttpAction action) {
        ActionLib.setCommonHeadersForOptions(action);
        action.getResponse().setHeader(HttpNames.hAllow, "OPTIONS,POST");
        action.getResponse().setHeader(HttpNames.hContentLength, "0");
    }

    protected void validate(HttpAction action) {
        String method = action.getRequest().getMethod();
        switch(method) {
            case HttpNames.METHOD_POST:
                break;
            default:
                ServletOps.errorMethodNotAllowed(method+" : Patch must use POST");
        }
        String ctStr = action.getRequest().getContentType();
        // Must be UTF-8 or unset. But this is wrong so often,
        // it is less trouble to just force UTF-8.
        String charset = action.getRequest().getCharacterEncoding();
        if ( charset != null && ! WebContent.charsetUTF8.equals(charset) )
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "Charset must be omitted or must be UTF-8, not "+charset);

        // If no header Content-type - assume patch-text.
        ContentType contentType = ( ctStr != null ) ? ContentType.create(ctStr) : ctPatchText;
        if ( ! ctPatchText.equals(contentType) && ! ctPatchBinary.equals(contentType) )
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "Allowed Content-types are "+ctPatchText+" or "+ctPatchBinary+", not "+ctStr);
        if ( ctPatchBinary.equals(contentType) )
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, contentTypePatchBinary+" not supported yet");
    }

    protected void operation(HttpAction action) {
        //incCounter(action.getEndpoint(), counterPatches);
        try {
            operation$(action);
            //incCounter(action.getEndpoint(), counterPatchesGood) ;
        } catch ( ActionErrorException ex ) {
            //incCounter(action.getEndpoint(), counterPatchesBad) ;
            throw ex ;
        }
    }

    private void operation$(HttpAction action) {
        //action.log.info(format("[%d] RDF Patch Write", action.id));
        try {
            actOnRDFPatch(action);
        } catch (Exception ex) {
            throw ex;
        }
        ServletOps.success(action);
    }

    private void actOnRDFPatch(HttpAction action) {
        try {
            String ct = action.getRequest().getContentType();
            InputStream input = action.getRequestInputStream();

            RDFPatch patch = RDFPatchOps.read(input);
            try ( OutputStream out = output.output() ) {
                String fn = output.currentFilename().getFileName().toString();
                if ( action.verbose ) {
                    Node id = patch.getId();
                    String idStr = id == null ? "<unset>" : NodeFmtLib.strNT(id);
                    action.log.info(format("[%d] Log: %s ==>> %s", action.id, idStr, fn));
                } else {
                    action.log.info(format("[%d] Log: %s", action.id, fn));
                }
                RDFPatchOps.write(out, patch);
            }
            ServletOps.success(action);
        }
        catch (RiotException ex) {
            ServletOps.errorBadRequest("RDF Patch parse error: "+ex.getMessage());
        }
        catch (IOException ex) {
            ServletOps.errorBadRequest("IOException: "+ex.getMessage());
        }
    }
}
