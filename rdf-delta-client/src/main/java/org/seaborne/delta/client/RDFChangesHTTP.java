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

package org.seaborne.delta.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse ;
import org.apache.http.StatusLine ;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.io.IndentedWriter ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaHttpException ;
import org.seaborne.delta.DeltaOps;
import org.seaborne.delta.Id ;
import org.seaborne.delta.lib.IOX ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesWriter;
import org.slf4j.Logger;

/** Collect the bytes of a change stream, then write to HTTP */ 
public class RDFChangesHTTP extends RDFChangesWriter {
    
    // This should be tied to the DeltaLink and have that control text/binary.
    
    private static final Logger LOG = Delta.DELTA_HTTP_LOG;
    // XXX Caching? Auth?
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final ByteArrayOutputStream bytes ;
    private final String url;
    private final String label ;
    // Used to coordinate with reading patches in.
    private final Object syncObject;
    private StatusLine statusLine       = null;
    private String response             = null;
    private Node patchId                = null;
    private boolean changeOccurred      = false;
    
    public RDFChangesHTTP(String label, String url) {
        this(label, null, url);
    }
    
    public RDFChangesHTTP(String label, Object syncObject, String url) {
        this(label, syncObject, url, new ByteArrayOutputStream(100*1024));
    }

    private RDFChangesHTTP(String label, Object syncObject, String url, ByteArrayOutputStream out) {
        super(DeltaOps.tokenWriter(out));
        // XXX When channels come in, this needs sorting out.
        this.syncObject = (syncObject!=null) ? syncObject : new Object(); 
        this.url = url;
        if ( label == null )
            label = url;
        this.label = label;
        this.bytes = out;
        reset();
    }
    
    @Override
    public void header(String field, Node value) {
        super.header(field, value);
        if ( field.equals(RDFPatch.ID) )
            patchId = value;
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        markChanged();
        super.add(g, s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        markChanged();
        super.delete(g, s, p, o);
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        markChanged();
        super.addPrefix(gn, prefix, uriStr);
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        markChanged();
        super.deletePrefix(gn, prefix);
    }

    @Override
    public void txnBegin() {
//        if ( currentTransactionId == null ) {
//            currentTransactionId = Id.create().asNode();
//            super.header(RDFPatch.ID, currentTransactionId);
//        }
        changeOccurred = false ;
        super.txnBegin();
    }

    @Override
    public void txnCommit() {
        super.txnCommit();
        send();
    }

    @Override
    public void txnAbort() {
        reset();
        // Forget.
    }
    
    private void markChanged() {
        changeOccurred = true;
    }

    private boolean changed() {
        return changeOccurred;
    }

    private void reset() {
        patchId = null ;
        changeOccurred = false ;
        bytes.reset();
    }

    private byte[] collected() {
        return bytes.toByteArray();
    }
    
    public void send() {
        synchronized(syncObject) {
            send$();
        }
    }
    
    /** Get the protocol response - may be null */
    public String getResponse() {
        return response;
    }
    
//    /** An {@link HttpEntity} that is "output only"; it writes a RDF Patch
//     * to an {@code OutputStream}.  
//     * It does not support {@link HttpEntity#getContent()} (currently!).  
//     */
//    static class HttpEntityRDFChanges extends AbstractHttpEntity {
//        // Open the connection on begin()
//        
//        // Really want to get to the base OutputStream.
//        // Output only?
//
//        RDFChangesWriter w;
//        
//        public HttpEntityRDFChanges(RDFChanges changes) {}
//        
//        @Override
//        public boolean isRepeatable() {
//            return false;
//        }
//
//        @Override
//        public long getContentLength() {
//            return -1;
//        }
//
//        @Override
//        public InputStream getContent() throws IOException, UnsupportedOperationException {
//            throw new UnsupportedOperationException("HttpEntityRDFChanges");
//        }
//
//        @Override
//        public void writeTo(OutputStream outstream) throws IOException {
//
//        }
//
//        @Override
//        public boolean isStreaming() {
//            return false;
//        }
//    }
    
    private void send$() {
        String idStr = Id.str(patchId);
        HttpPost postRequest = new HttpPost(url);
        byte[] bytes = collected();

        FmtLog.info(LOG, "Send patch %s (%d bytes) -> %s", idStr, bytes.length, label);
        
        if ( false ) {
            if ( LOG.isDebugEnabled() ) {
                // Ouch.
                String s = new String(bytes, StandardCharsets.UTF_8);
                LOG.debug("== Sending ...");
                // Do NOT close!
                IndentedWriter w = IndentedWriter.stdout;
                String x = w.getLinePrefix();
                w.setLinePrefix(">> ");
                w.print(s);
                w.setLinePrefix(x);
                if ( ! s.endsWith("\n") )
                    w.println();
                w.flush();
                LOG.debug("== ==");
            }
        }
        postRequest.setEntity(new ByteArrayEntity(bytes));

        try(CloseableHttpResponse r = httpClient.execute(postRequest) ) {
            statusLine = r.getStatusLine();
            response = readResponse(r);
            int sc = r.getStatusLine().getStatusCode();
            if ( sc >= 200 && sc <= 299 )
                return ;
            if ( sc >= 300 && sc <= 399 ) {
                FmtLog.info(LOG, "Send patch %s HTTP %d", idStr, sc);
                throw new DeltaHttpException(sc, "HTTP Redirect");
            }
            if ( sc >= 400 && sc <= 499 )
                throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
            if ( sc >= 500 )
                throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
        } catch (IOException e) { throw IOX.exception(e); }
        reset(); 
    }
        
    private static String readResponse(HttpResponse resp) {
        HttpEntity e = resp.getEntity();
        if ( e != null ) {
            try ( InputStream ins = e.getContent() ) {
                return IO.readWholeFileAsUTF8(ins);
            } catch (IOException ex) { ex.printStackTrace(); }
        } 
        return null;
    }
}
