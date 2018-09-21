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

import java.util.Objects;

public class S3Config {
    public static S3Config.Builder create() { return new Builder(); }

    final String s3BucketName;
    final String s3Region;
    final String s3CredentialsFile;
    final String s3CredentialsProfile;
    final String s3Endpoint;

    public S3Config(String s3BucketName, String s3Region, String s3CredentialsFile, String s3CredentialsProfile, String s3Endpoint) {
        super();
        this.s3BucketName = s3BucketName;
        this.s3Region = s3Region;
        this.s3CredentialsFile = s3CredentialsFile;
        this.s3CredentialsProfile = s3CredentialsProfile;
        this.s3Endpoint = s3Endpoint;
    }

    public static class Builder {
        public String s3BucketName         = null;
        public String s3Region             = null;
        public String s3CredentialsFile    = null;
        public String s3CredentialsProfile = null;
        public String s3Endpoint           = null;

        Builder() {}

        public S3Config.Builder bucketName(String bucket) {
            s3BucketName = bucket;
            return this;
        }

        public S3Config.Builder region(String region) {
            s3Region = region;
            return this;
        }

        public S3Config.Builder credentialsFile(String credentialsFile) {
            s3CredentialsFile = credentialsFile;
            return this;
        }

        public S3Config.Builder credentialsProfile(String credentialsProfile) {
            s3CredentialsProfile = credentialsProfile;
            return this;
        }

        public S3Config.Builder endpoint(String endpoint) {
            s3Endpoint = endpoint;
            return this;
        }

        public S3Config build() {
            Objects.requireNonNull(s3BucketName);
            Objects.requireNonNull(s3Region);
            if ( s3CredentialsProfile != null )
                Objects.requireNonNull(s3CredentialsFile);
            return new S3Config(s3BucketName, s3Region, s3CredentialsFile, s3CredentialsProfile, s3Endpoint);
        }
    }
}