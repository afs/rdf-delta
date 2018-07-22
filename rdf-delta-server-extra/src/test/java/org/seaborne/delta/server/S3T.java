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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

import io.findify.s3mock.S3Mock;
import org.apache.curator.test.TestingServer;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.s3.PatchStoreProviderZkS3;
import org.seaborne.delta.server.s3.S3;

public class S3T {
    
    private static String BUCKET_NAME = "some-bucket";
    
    public static PatchStore setup() {
        TestingServer server = ZkT.localServer();
        String connectString = "localhost:" + server.getPort();
        String endpoint = S3T.makeMockS3();
        LocalServerConfig config = S3.configZkS3(connectString, BUCKET_NAME, "eu-west-1", endpoint);
        
        PatchStoreProvider provider = new PatchStoreProviderZkS3();
        PatchStore patchStore = provider.create(config);
        patchStore.initialize(new DataRegistry("X"), config);
        return patchStore;
    }
    
    public static String makeMockS3() {
        int port = LibX.choosePort();
        AWSCredentials credentials = new AnonymousAWSCredentials();
        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:"+port, "us-west-2");
    
        S3Mock api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        api.start();
        return endpoint.getServiceEndpoint();
        
//        return AmazonS3ClientBuilder
//            .standard()
//            .withPathStyleAccessEnabled(true)  
//            .withEndpointConfiguration(endpoint)
//            .withCredentials(new AWSStaticCredentialsProvider(credentials))     
//            .build();
    }

}
