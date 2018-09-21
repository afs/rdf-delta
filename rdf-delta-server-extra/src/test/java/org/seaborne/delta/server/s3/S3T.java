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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

import io.findify.s3mock.S3Mock;
import org.apache.curator.test.TestingServer;
import org.apache.jena.atlas.lib.Pair;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.server.ZkT;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;

public class S3T {

    static String REGION = "eu-bristol-1";
    static String BUCKET_NAME = "some-bucket";

    public static Pair<PatchStore, S3Mock> setup() {
        TestingServer server = ZkT.localServer();
        String connectString = "localhost:" + server.getPort();
        int port = LibX.choosePort();

        S3Mock api = new S3Mock.Builder()
            .withPort(port)
            .withInMemoryBackend().build();
        // Must start so provider.create works.
        api.start();

//      AWSCredentials credentials = new AnonymousAWSCredentials();
//      AmazonS3ClientBuilder
//          .standard()
//          .withPathStyleAccessEnabled(true)
//          .withEndpointConfiguration(endpoint)
//          .withCredentials(new AWSStaticCredentialsProvider(credentials))
//          .build();

        EndpointConfiguration endpointCfg = new EndpointConfiguration("http://localhost:"+port+"/", REGION);
        String endpoint = endpointCfg.getServiceEndpoint();

        S3Config cfg = S3Config.create()
            .bucketName(BUCKET_NAME)
            .region(REGION)
            .endpoint(endpoint)
            .build();
        LocalServerConfig config = S3.configZkS3(connectString, cfg);

        PatchStoreProvider provider = new PatchStoreProviderZkS3();
        PatchStore patchStore = provider.create(config);
        patchStore.initialize(new DataRegistry("X"), config);
        return Pair.create(patchStore, api);
    }

}
