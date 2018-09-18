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

import static org.seaborne.delta.server.s3.S3Const.pBucketName;
import static org.seaborne.delta.server.s3.S3Const.pPrefix;
import static org.seaborne.delta.server.s3.S3Const.pRegion;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.util.StringUtils;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;

public class PatchStoreProviderZkS3 extends PatchStoreProviderZk {
    public static String convertProviderName(String logProvider) {
        return logProvider+"_S3";
    }

    public static final String ProviderName = convertProviderName(DPS.PatchStoreZkProvider);

    private static String DEFAULT_PREFIX = "patches/";
    private Map<String, DetailsS3> access = new ConcurrentHashMap<>();

    static class DetailsS3 {
        final String prefix;
        final String bucketName;  //DNS name,lower case.
        final AmazonS3 client;

        public DetailsS3(String bucketName, String prefix, AmazonS3 client) {
            this.prefix = prefix;
            this.bucketName = bucketName;
            this.client = client;
        }
    }

    public PatchStoreProviderZkS3() {}

    private DetailsS3 accessS3(LocalServerConfig configuration) {
        // Access and checking.
        String bucketName = configuration.getProperty(pBucketName);
        if ( StringUtils.isNullOrEmpty(bucketName) )
            throw new IllegalArgumentException("Missing required property: "+pBucketName);

        String region = configuration.getProperty(pRegion);
        if ( StringUtils.isNullOrEmpty(region) )
            throw new IllegalArgumentException("Missing required property: "+pRegion);

        String prefixStr = configuration.getProperty(pPrefix);
        if ( StringUtils.isNullOrEmpty(prefixStr) )
            prefixStr = DEFAULT_PREFIX;
        String prefix = (prefixStr!=null) ? prefixStr : DEFAULT_PREFIX;
        AmazonS3 client = S3.buildS3(configuration);
        return new DetailsS3(bucketName, prefix, client);
        //return access.computeIfAbsent(bucketName, n->new DetailsS3(bucketName, prefix, client));
    }

    /** Long name */
    @Override
    public String getProviderName() {
        return ProviderName;
    }

    /** Short name used in server configuration files to set the default provider via "log_type" */
    @Override
    public String getShortName() {
        return super.getShortName() + "s3";
    }

    @Override
    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        DetailsS3 s3 = accessS3(configuration);
        String logPrefix = s3.prefix+dsd.getName()+"/";
        if ( ! s3.client.doesBucketExistV2(s3.bucketName) ) {
            s3.client.createBucket(s3.bucketName);
        }
        return new PatchStorageS3(s3.client, s3.bucketName, logPrefix);
    }
}
