/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.s3;


import static org.seaborne.delta.server.s3.S3Const.*;

import java.util.Objects;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;

import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/** Library of fucntiosn relating to S3 */
public class S3 {
    /**
     * Create {@link LocalServerConfig} for a Zookeeper and S3 based patch log server.
     */
    public static LocalServerConfig configZkS3(String zkConnectionString, S3Config s3Cfg) {
        LocalServerConfig c = LocalServers.configZk(zkConnectionString);
        return configZkS3(c, s3Cfg);
    }

    /**
     * Create {@link LocalServerConfig} for a S3 based patch log server,
     * starting with the LocalServerConfig for the index server.
     */
    public static LocalServerConfig configZkS3(LocalServerConfig c, S3Config s3Cfg) {
        Properties properties = cfg2properties(s3Cfg);
        return LocalServerConfig.create(c)
            .setLogProvider(PatchStoreProviderZkS3.ProviderName)
            .setProperties(properties)
            .build();
    }

    private static  Properties cfg2properties(S3Config s3Cfg) {
        return PropertiesBuilder.create()
            .set(pBucketName, s3Cfg.s3BucketName)
            .set(pRegion, s3Cfg.s3Region)
            .option(pEndpoint, s3Cfg.s3Endpoint)
            .option(pCredentialFile, s3Cfg.s3CredentialsFile)
            .option(pCredentialProfile, s3Cfg.s3CredentialsProfile)
            .build();
    }

    // Anon credentials and provider - used in S3Mock to ensure non default AWS credentials mechanism is used in tests.
    private static AWSCredentialsProvider anon = new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());

    public static AmazonS3 buildS3(LocalServerConfig configuration) {
        String region = configuration.getProperty(pRegion);
        String endpoint = configuration.getProperty(pEndpoint);
        String credentialsFile =  configuration.getProperty(pCredentialFile);
        String credentialsProfile =  configuration.getProperty(pCredentialProfile);

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if ( endpoint == null )
            builder.withRegion(region);
        else  {
            // Needed for S3mock
            builder.withPathStyleAccessEnabled(true);
            builder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
            builder.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
        }
        if ( credentialsFile != null )
            builder.withCredentials(new ProfileCredentialsProvider(credentialsFile, credentialsProfile));
        return builder.build();
    }

    /** Test whether the bucket exists and is accessible. */
    public static void ensureBucketExists(AmazonS3 client, String bucketName) {
        if ( ! bucketExists(client, bucketName) )
            createBucket(client, bucketName);
    }

    /** Test whether the bucket exists and is accessible. */
    public static boolean bucketExists(AmazonS3 client, String bucketName) {
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

    public static void createBucket(AmazonS3 client, String bucketName) {
        Bucket bucket = client.createBucket(bucketName);
    }

    private static class PropertiesBuilder {

        public static PropertiesBuilder create() { return new PropertiesBuilder(); }

        private Properties properties = new Properties();

        public PropertiesBuilder set(String propertyName, String propertyValue) {
            Objects.requireNonNull(propertyName, "propertyName");
            if ( propertyValue == null )
                throw new DeltaConfigException("Null value for '"+propertyName+"'");
            properties.setProperty(propertyName, propertyValue);
            return this;
        }

        public PropertiesBuilder option(String propertyName, String propertyValue) {
            Objects.requireNonNull(propertyName, "propertyName");
            if ( propertyValue != null )
                properties.setProperty(propertyName, propertyValue);
            return this;
        }

        public Properties build() {
            return properties;
        }
    }
}
