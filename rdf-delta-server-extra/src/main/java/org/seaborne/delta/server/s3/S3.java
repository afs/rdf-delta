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

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.seaborne.delta.lib.LibX;

public class S3 {
    
    private static AmazonS3 makeS3(String credentialFile, String region) {
        try {
            AWSCredentials credentials = new PropertiesCredentials(new File(credentialFile));
            AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                // Why not ~/.aws/credentials? No environment variables?
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
            return client;
        } catch (IllegalArgumentException | IOException e) {
            throw LibX.adapt(e); 
        }
    }
}
