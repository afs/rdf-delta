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

package org.seaborne.delta.server;

import java.io.IOException;
import java.net.ServerSocket;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Region;

import io.findify.s3mock.S3Mock;
import org.apache.jena.atlas.logging.LogCtl;
import org.junit.After;
import org.junit.Before;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.patchstores.AbstractTestPatchStorage;
import org.seaborne.delta.server.s3.PatchStorageS3;

public class TestPatchStorageS3 extends AbstractTestPatchStorage {
    static { LogCtl.setJavaLogging(); }
    
    private static String testBucketName = "delta";
    private static String testPrefix = "patches";
    
    private static int port = choosePort();
    private static String serviceEndpoint = "http://localhost:"+port;
    private static String signingRegion = Region.US_West.name();
    private static EndpointConfiguration endpoint = new EndpointConfiguration(serviceEndpoint, signingRegion);

    private AmazonS3 client;
    private S3Mock s3Mock;
    
    /** Choose an unused port for a server to listen on */
    public static int choosePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException ex) {
            throw new DeltaException("Failed to find a port");
        }
    }
    
    @Before public void before() {
        s3Mock = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        s3Mock.start();
                
        /* AWS S3 client setup.
         *  withPathStyleAccessEnabled(true) trick is required to overcome S3 default 
         *  DNS-based bucket access scheme
         *  resulting in attempts to connect to addresses like "bucketname.localhost"
         *  which requires specific DNS setup.
         */
        client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)  
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))     
            .build();
   }
    
    @After public void after() {
        client.shutdown();
        s3Mock.shutdown();
    }
    
    
    @Override
    protected PatchStorage patchStorage() {
        return new PatchStorageS3(client, testBucketName, testPrefix);
    }
    
//    @Test 
}
