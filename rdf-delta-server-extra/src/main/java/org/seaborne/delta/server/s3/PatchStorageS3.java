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

package org.seaborne.delta.server.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Objects;
import java.util.stream.Stream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public class PatchStorageS3 implements PatchStorage {
    
    private final AmazonS3 client;
    private String bucketName;
    private String prefix;
    
    public PatchStorageS3(AmazonS3 client, String bucketName, String prefix) {
        this.client = client;
        this.bucketName = bucketName;
        if ( ! prefix.endsWith("/") )
            prefix = prefix+"/";
        this.prefix = prefix;
        init(client, bucketName, prefix);
    }
    
    private static void init(AmazonS3 client, String bucketName, String prefix) {
        if ( ! bucketExists(client, bucketName) )
            createBucket(client, bucketName);
    }

    /** Test whether the bucket exists and is accessible. */  
    private static boolean bucketExists(AmazonS3 client, String bucketName) {
        try {
            HeadBucketRequest request = new HeadBucketRequest(bucketName);
            HeadBucketResult result = client.headBucket(request);
            return true;
        }
        catch (AmazonServiceException awsEx) {
            switch (awsEx.getStatusCode()) {
                case HttpSC.NOT_FOUND_404 :
                    return false;
                case HttpSC.FORBIDDEN_403 :
                    break;
                case HttpSC.MOVED_PERMANENTLY_301 : { // Moved permanently.
                    System.err.println("301 Location: " + awsEx.getHttpHeaders().get(HttpNames.hLocation));
                    break;
                }
            }
            throw awsEx;
        }
    }
    
    private static void createBucket(AmazonS3 client, String bucketName) {
        Bucket bucket = client.createBucket(bucketName);
    }
    
    private String idToKey(Id id) {
        return prefix+id.asParam();
    }

    @Override
    public Stream<Id> find() {
        ObjectListing objects = client.listObjects(bucketName, prefix);
        return
            objects.getObjectSummaries().stream()
                .map(s->keyToId(s))
                .filter(Objects::nonNull);
    }

    private Id keyToId(S3ObjectSummary summary) {
        String awsKey = summary.getKey();
        if ( ! awsKey.startsWith(prefix) ) {
            Log.warn(this, "Not a good object name: "+awsKey);
            return null;
        }
        String x = awsKey.substring(prefix.length());
        try { 
            return Id.fromString(x); 
        } catch ( IllegalArgumentException ex ) {
            Log.warn(this, "Not a S3 key for a patch id: "+awsKey);
            return null;
        }
    }

    @Override
    public void store(Id key, RDFPatch value) {
        String s3Key = idToKey(key);
        ByteArrayOutputStream out = new ByteArrayOutputStream(10*1024); 
        RDFPatchOps.write(out, value);
        byte[] bytes = out.toByteArray();
        InputStream in = new ByteArrayInputStream(bytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(DeltaConst.contentTypePatchText);
        metadata.setContentLength(bytes.length);
        client.putObject(bucketName, s3Key, in, metadata);
    }

    @Override
    public RDFPatch fetch(Id key) {
        String s3Key = idToKey(key);
        try {
            S3Object x = client.getObject(bucketName, s3Key);
            x.getObjectMetadata();
            S3ObjectInputStream input = x.getObjectContent();
            RDFPatch patch = RDFPatchOps.read(input);
            return patch;
        }
        catch (AmazonServiceException awsEx) {
            switch (awsEx.getStatusCode()) {
                case HttpSC.NOT_FOUND_404 :
                case HttpSC.FORBIDDEN_403 :
                    return null;
                case HttpSC.MOVED_PERMANENTLY_301 : { // Moved permanently.
                    System.err.println("301 Location: " + awsEx.getHttpHeaders().get(HttpNames.hLocation));
                    return null;
                }
            }
            throw awsEx;
        }

    }

    @Override
    public void delete(Id id) {}
}
