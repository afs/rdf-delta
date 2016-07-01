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
import java.nio.charset.StandardCharsets ;

import org.apache.http.client.methods.CloseableHttpResponse ;
import org.apache.http.client.methods.HttpPost ;
import org.apache.http.entity.ByteArrayEntity ;
import org.apache.http.impl.client.CloseableHttpClient ;
import org.apache.http.impl.client.HttpClients ;
import org.seaborne.delta.base.StreamChangesWriter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Collect the bytes of a chnage stream, then write to HTTP */ 
public class StreamChangesCollect extends StreamChangesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamChangesCollect.class) ;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final ByteArrayOutputStream bytes  ;
    private final String url ;

    public StreamChangesCollect(String url) {
        this(url, new ByteArrayOutputStream(100*1024)) ;
    }
    
    private StreamChangesCollect(String url, ByteArrayOutputStream out) {
        super(out) ;
        this.url = url ;
        this.bytes = out ;
    }
    
    @Override
    public void start() { 
        
    }

    
    byte[] collected() { 
        return bytes.toByteArray() ;
    }
    
    public void send() {
        HttpPost postRequest = new HttpPost(url) ;
        byte[] bytes = collected() ;
        String s = new String(bytes, StandardCharsets.UTF_8) ;
        if ( false ) {
            System.out.println("== Sending ...") ;
            System.out.print(s) ;
            if ( ! s.endsWith("\n") )
                System.out.println() ;
            System.out.println("== ==") ;
        }
        postRequest.setEntity(new ByteArrayEntity(bytes)) ;

        try(CloseableHttpResponse r = httpClient.execute(postRequest)) {
            int sc = r.getStatusLine().getStatusCode() ;
            if ( sc < 200 || sc >= 300 )
                LOG.warn("HTTP response: "+r.getStatusLine()) ;
        }
        catch (IOException e) { e.printStackTrace(); }
        this.bytes.reset(); 
    }
}
