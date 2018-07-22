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

/**
 * Properties:
 *   delta.s3.endpoint      Provide the endpoint (for mocking)
 *   
 *   delta.s3.region        Required.
 *   delta.s3.bucket_name   Required.
 *   delta.s3.prefix        Defaults to "patches/"
 *   
 *   The default S3CredentialsProviderChain is used unless: 
 *   delta.s3.credentials_file
 *   delta.s3.credentials_profile
 *   in which case 
 *   
 */
public class S3Const {
    // AWS Docs: 
    // https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html
    
    // AWS S3 layout
    public static final String   pBucketName        = "delta.s3.bucket_name";
    public static final String   pPrefix            = "delta.s3.prefix";

    // AWS access
    public static final String   pEndpoint          = "delta.s3.endpoint";
    public static final String   pRegion            = "delta.s3.region";
    public static final String   pCredentialFile    = "delta.s3.credentials_file";
    public static final String   pCredentialProfile = "delta.s3.credentials_profile";    
}
