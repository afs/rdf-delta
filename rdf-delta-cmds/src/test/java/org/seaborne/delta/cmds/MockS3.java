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

package org.seaborne.delta.cmds;

import io.findify.s3mock.S3Mock;
import org.apache.jena.atlas.lib.Lib;

/** Run a mock S3 using io.findify */
public class MockS3 {
    public static void main(String... args) {
        int PORT = 1357;
        String region = "eu-bristol-1";
////      AWSCredentials credentials = new AnonymousAWSCredentials();
////      EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:"+port, region);
        S3Mock api = new S3Mock.Builder().withPort(PORT).withInMemoryBackend().build();
        api.start();
        System.out.println("MockS3 running");
        while(true) {
            Lib.sleep(1000);
        }
    }
}
