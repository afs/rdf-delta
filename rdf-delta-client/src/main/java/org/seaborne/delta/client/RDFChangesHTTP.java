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
import java.util.Arrays;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DeltaOps;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.changes.RDFChangesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collect the bytes of a change stream, then write to HTTP */ 
public class RDFChangesHTTP extends RDFChangesWriter {
    
    // This should be tied to the DeltaLink and have that control text/binary.
    
    private static final Logger LOG = LoggerFactory.getLogger(RDFChangesHTTP.class);
    // XXX Caching? Auth?
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final ByteArrayOutputStream bytes ;
    private final String url;
    // Used to coordinate with reading paches in.
    private final Object syncObject;
    private Node currentTransactionId = null;
    
    // XXX Text token specific.
    private static final byte[] noChange =  "TB .\nTC .\n".getBytes(StandardCharsets.UTF_8);

    public RDFChangesHTTP(String url) {
        this(null, url);
    }
    
    public RDFChangesHTTP(Object syncObject, String url) {
        this(syncObject, url, new ByteArrayOutputStream(100*1024));
    }

    private RDFChangesHTTP(Object syncObject, String url, ByteArrayOutputStream out) {
        super(DeltaOps.tokenWriter(out));
        // XXX When channels come in, this needs sorting out.
        this.syncObject = (syncObject!=null) ? syncObject : new Object(); 
        this.url = url;
        this.bytes = out;
        reset();
    }
    
    @Override
    public void txnBegin() {
        super.txnBegin();
        if ( currentTransactionId == null ) {
            currentTransactionId = Id.create().asNode();
            super.header(RDFPatch.ID, currentTransactionId);
        }
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
    
    private void reset() {
        currentTransactionId = null ;
        bytes.reset();
    }

    private byte[] collected() {
        return bytes.toByteArray();
    }
    
    // XXX Per channel.
    
    public void send() {
        synchronized(syncObject) {
            send$();
        }
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
        HttpPost postRequest = new HttpPost(url);
        byte[] bytes = collected();
        // XXX better way elsewhere to determine TB-TC. 
        if ( Arrays.equals(bytes, noChange) ) {
            reset();
            LOG.info("Skip TB-TC no chanage");
            // Skip TB,TC.
            return; 
        }

        String s = new String(bytes, StandardCharsets.UTF_8);
        if ( true ) {
            LOG.info("== Sending ...");
            System.out.print(s);
            if ( ! s.endsWith("\n") )
                System.out.println();
            LOG.info("== ==");
        }
        FmtLog.info(LOG, "Send patch (%d bytes)", bytes.length);
        postRequest.setEntity(new ByteArrayEntity(bytes));

        try(CloseableHttpResponse r = httpClient.execute(postRequest) ) {
            int sc = r.getStatusLine().getStatusCode();
            if ( sc < 200 || sc >= 300 )
                LOG.warn("HTTP response: "+r.getStatusLine()+" ("+url+")");
            else { 
                HttpEntity e = r.getEntity();
                if ( e != null )
                    handleJsonResponse(e);
            }
        } catch (IOException e) { e.printStackTrace(); }
        // Notify of send.
        reset(); 
    }

    private void handleJsonResponse(HttpEntity e) {
        try ( InputStream ins = e.getContent() ) {
            // If there is a JSON object reply.
            // Roll this into DRPC: stream->json; json->stream; stream->stream
            if ( ins.available() > 0 ) {
                JsonObject obj = JSON.parse(ins);
                String x = JSON.toStringFlat(obj);
                LOG.info("HTTP reply: "+x);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }
}
