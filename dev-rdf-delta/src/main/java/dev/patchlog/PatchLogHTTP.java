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

package dev.patchlog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.DeltaHttpException;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchLogHTTP implements PatchLogSimpler {
    private static Logger LOG = LoggerFactory.getLogger(PatchLogHTTP.class);
    private final String url;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private RegToken regToken;

    public PatchLogHTTP(String url, RegToken regToken) {
        if ( ! url.endsWith("/") )
            url = url + "/";
        this.url = url;
        this.regToken = regToken;
    }

    // DRY: DeltaLinkHTTP
    
    @Override
    public long append(String dsName, RDFPatch patch) {
        String idStr = Id.str(patch.getId());
        
        // Need registration.
        String s = url+dsName+"?token="+regToken.asString();
        
        ByteArrayOutputStream out = new ByteArrayOutputStream(100*1024);
        
        RDFPatchOps.write(out, patch);
        HttpPost postRequest = new HttpPost(s);
        byte[] bytes = out.toByteArray();
        postRequest.setEntity(new ByteArrayEntity(bytes));

        String response;
        try(CloseableHttpResponse r = httpClient.execute(postRequest) ) {
            StatusLine statusLine = r.getStatusLine();
            response = readResponse(r);
            int sc = r.getStatusLine().getStatusCode();
            if ( sc <= 199 ) {}
            
            if ( sc >= 300 ) {
                if ( sc >= 300 && sc <= 399 ) {
                    FmtLog.info(LOG, "Send patch %s HTTP %d", idStr, sc);
                    throw new DeltaHttpException(sc, "HTTP Redirect");
                }
                if ( sc >= 400 && sc <= 499 )
                    throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
                if ( sc >= 500 )
                    throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
            }
        } catch (IOException e) { throw IOX.exception(e); }
        
        if ( response != null ) {
            try {
                JsonObject obj = JSON.parse(response);
                int version = obj.get(DeltaConst.F_VERSION).getAsNumber().value().intValue();
                return version;
            } catch (Exception ex) { ex.printStackTrace(); }
        }
        return 0;
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

    @Override
    public RDFPatch fetch(String dsName, long version) {
        return fetchCommon(dsName, Long.toString(version));
    }

    @Override
    public RDFPatch fetch(String dsName, Id patchId) {
        return fetchCommon(dsName, patchId.asPlainString());
    }

    @Override
    public PatchLogInfo getPatchLogInfoByName(String dsName) {
        return null;
    }
    
    private RDFPatch fetchCommon(String dsName, String string) {
        // Wrong by documentation.
        String s = url+dsName+"/"+string;
        HttpGet request = new HttpGet(s);
        
        try(CloseableHttpResponse r = httpClient.execute(request) ) {
            int sc = r.getStatusLine().getStatusCode();
            if ( sc >= 200 && sc <= 299 )
                return RDFPatchOps.read(r.getEntity().getContent());
            if ( sc >= 300 && sc <= 399 ) {
                throw new DeltaHttpException(sc, "HTTP Redirect");
            }
            if ( sc >= 400 && sc <= 499 )
                throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
            if ( sc >= 500 )
                throw new DeltaHttpException(sc, r.getStatusLine().getReasonPhrase());
        } catch (IOException e) { throw IOX.exception(e); }
        return null;
    }
}
