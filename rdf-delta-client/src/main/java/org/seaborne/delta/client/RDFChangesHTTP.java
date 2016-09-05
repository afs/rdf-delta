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

import java.io.ByteArrayOutputStream ;
import java.io.IOException ;
import java.io.InputStream ;
import java.nio.charset.StandardCharsets ;
import java.util.Arrays ;

import org.apache.http.client.methods.CloseableHttpResponse ;
import org.apache.http.client.methods.HttpPost ;
import org.apache.http.entity.ByteArrayEntity ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.seaborne.patch.RDFChangesWriter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Collect the bytes of a change stream, then write to HTTP */ 
public class RDFChangesHTTP extends RDFChangesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(RDFChangesHTTP.class) ;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final ByteArrayOutputStream bytes  ;
    private final String url ;
    // Used to coordinate with reading paches in.
    private final Object syncObject ;
    
    private static final byte[] noChange =  "TB .\nTC .\n".getBytes(StandardCharsets.UTF_8) ;

    public RDFChangesHTTP(String url) {
        this(null, url) ;
    }
    
    public RDFChangesHTTP(Object syncObject, String url) {
        this(syncObject, url, new ByteArrayOutputStream(100*1024)) ;
    }

    private RDFChangesHTTP(Object syncObject, String url, ByteArrayOutputStream out) {
        super(out) ;
        // XXX When channels come in, this needs sorting out.
        this.syncObject = (syncObject!=null) ? syncObject : new Object() ; 
        this.url = url ;
        this.bytes = out ;
        reset() ;
//        txnBegin(ReadWrite.WRITE) ;
//        txnCommit() ;
//        byte [] x = collected() ;
//        reset() ;
//        if ( Arrays.equals(noChange,x) ) {
//            LOG.warn("Calculated 'no change' not equal to reset 'no change'");
//        }
    }
    
    @Override
    public void start() { 
        
    }

//    @Override
//    public void txnBegin(ReadWrite mode) {
//        super.txnBegin(mode);
//    }

//    @Override
//    public void txnPromote() {
//        super.txnPromote(); 
//    }

    @Override
    public void txnCommit() {
        super.txnCommit();
        send() ;
    }

    @Override
    public void txnAbort() {
        reset() ;
    }
    
    private void reset() {
        bytes.reset() ;
    }

    private byte[] collected() {
        return bytes.toByteArray() ;
    }
    
    // XXX Per channel.
    
    public void send() {
        synchronized(syncObject) {
            send$() ;
        }
    }

    private void send$() {
        HttpPost postRequest = new HttpPost(url) ;
        byte[] bytes = collected() ;
        // XXX better way elsewhere to determine TB-TC. 
        if ( Arrays.equals(bytes, noChange) ) {
            reset() ;
            LOG.info("Skip TB-TC no chanage") ;
            // Skip TB,TC.
            return ; 
        }

        String s = new String(bytes, StandardCharsets.UTF_8) ;
        if ( true ) {
            LOG.info("== Sending ...") ;
            System.out.print(s) ;
            if ( ! s.endsWith("\n") )
                System.out.println() ;
            LOG.info("== ==") ;
        }
        FmtLog.info(LOG, "Send patch (%d bytes)", bytes.length) ;
        postRequest.setEntity(new ByteArrayEntity(bytes)) ;

        try(CloseableHttpResponse r = httpClient.execute(postRequest) ; 
            InputStream ins = r.getEntity().getContent() ;
            ) {
            // If there is a JSON object reply.
            // Roll this into DRPC: stream->json ; json->stream ; stream->stream
            if ( ins.available() > 0 ) {
                JsonObject obj = JSON.parse(ins) ;
                String x = JSON.toStringFlat(obj) ;
                LOG.info("HTTP reply: "+x) ;
            }
            int sc = r.getStatusLine().getStatusCode() ;
            if ( sc < 200 || sc >= 300 )
                LOG.warn("HTTP response: "+r.getStatusLine()+" ("+url+")") ;
        }
        catch (IOException e) { e.printStackTrace(); }
        // Notify of send.
        reset(); 
    }
}
