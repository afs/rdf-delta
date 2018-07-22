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

import com.amazonaws.services.s3.AmazonS3;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;

public class PatchStoreProviderZkS3 extends PatchStoreProviderZk {

    private final AmazonS3 s3Client;
    private static String bucketName = "rdf-delta-bucket";  //DNS name,lower case.
    private static String prefix = "patches/"; 

    public PatchStoreProviderZkS3(AmazonS3 s3Client) {
        // XXX Temporary until properties.
        this.s3Client = s3Client; 
    }
    
    //public PatchStoreProviderZkS3() {}
    
    /** Long name */ 
    @Override
    public String getProviderName() { return super.getProviderName()+"_S3"; }
    
    /** Short name used in server configuration files to set the default provider via "log_type" */ 
    @Override
    public String getShortName() { return super.getShortName()+"s3"; }
    
    @Override
    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        String logPrefix = prefix+dsd.getName()+"/";
        if ( ! s3Client.doesBucketExistV2(logPrefix) ) {
            s3Client.createBucket(bucketName);
        }
        return new PatchStorageS3(s3Client, bucketName, logPrefix);
    }
}
