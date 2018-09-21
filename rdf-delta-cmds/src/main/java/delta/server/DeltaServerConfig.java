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

package delta.server;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;

/**
 * DeltaServer configuration.
 * <p>
 * Once created the setting have been checked and can be
 * assumed to be correct and consistent without further checking.
 */
public class DeltaServerConfig {
    // Server level.
    // Either a port number xor a Jetty configuration file.
    // If there is a jetty.xml filename, the port is ignored.
    public Integer serverPort = null;
    public String jettyConf   = null;

    // Provider. Assumes necessary classes are on the classpath.
    public Provider provider = Provider.UNSET;

    // File provider
    public String fileBase = null ;

    // Zookeeper provider
    public String zkConnectionString = null ;

    // Co-hosted Zookeeper server type, if any.
    public ZkMode zkMode = ZkMode.NONE;

    // Zookeeper embedded full server
    public Integer zkPort = null;
    public String zkData = null;
    // Quorum ensemble
    public String zkConf = null;

    // S3 patch storage.
    public String s3BucketName = null;
    public String s3Region = null;
    public String s3CredentialsFile = null;
    public String s3CredentialsProfile = null;
    public String s3Endpoint = null;

    // ---- JSON field constants
    private static String fProvider           = "store";

    private static String fPort               = "port";
    private static String fJetty              = "jetty";

    // The Zookeeper provider
    private static String fZkConnectionString = "zk.connect";
    private static String fZkConfig           = "zk.config";
    private static String fZkPort             = "zk.port";
    private static String fZkData             = "zk.data";
    // The File provider
    private static String fFileDirData        = "filestore";
    // The memory provider
    // -- none
    // The S3 backend to Zookeeper
    private static String fS3BucketName       = "s3.bucket";
    private static String fS3Region           = "s3.region";
    private static String fS3CredsFile        = "s3.keys.file";
    private static String fS3CredsProfile     = "s3.keys.profile";
    private static String fS3Endpoint         = "s3.endpoint";

    // ---- JSON field constants

    public DeltaServerConfig( ) {}

    public static DeltaServerConfig read(String file) {
        JsonObject obj = JSON.read(file);
        return create(obj);
    }

    private static void validate(DeltaServerConfig conf) {}

    /** Recreate a {@code DeltaServerConfig} from JSON */
    public static DeltaServerConfig create(JsonObject obj) {
        DeltaServerConfig conf = new DeltaServerConfig();
        if ( obj.hasKey(fPort) ) {
            int x = JSONX.getInt(obj, fPort, -1);
            if ( x >= 0 )
                conf.serverPort = x;
        }
        if ( obj.hasKey(fJetty) )
            conf.jettyConf = JSONX.getStrOrNull(obj, fJetty);

        if ( obj.hasKey(fZkConnectionString) )
            conf.zkConnectionString = JSONX.getStrOrNull(obj, fZkConnectionString);

        if ( obj.hasKey(fZkConfig) )
            conf.zkConf = JSONX.getStrOrNull(obj, fZkConfig);

        if ( obj.hasKey(fZkPort) ) {
            int x = JSONX.getInt(obj, fZkPort, -1);
            if ( x >= 0 )
                conf.zkPort = x;
        }
        if ( obj.hasKey(fZkData) )
            conf.zkData = JSONX.getStrOrNull(obj, fZkData);

        if ( obj.hasKey(fFileDirData) )
            conf.fileBase = JSONX.getStrOrNull(obj, fFileDirData);

        if ( obj.hasKey(fProvider) )
            conf.provider = Provider.create(JSONX.getStrOrNull(obj, fProvider));

        if ( conf.zkConnectionString != null )
            conf.zkMode = ZkMode.EXTERNAL;

        if ( conf.zkConf != null )
            conf.zkMode = ZkMode.QUORUM;
        else if ( "mem".equalsIgnoreCase(conf.zkConnectionString) )
            conf.zkMode = ZkMode.MEM;
        else if ( conf.zkData != null )
            conf.zkMode = ZkMode.SINGLE;

        if ( obj.hasKey(fS3BucketName) )
            conf.s3BucketName = JSONX.getStrOrNull(obj, fS3BucketName);
        if ( obj.hasKey(fS3Region) )
            conf.s3Region = JSONX.getStrOrNull(obj, fS3Region);
        if ( obj.hasKey(fS3CredsFile) )
            conf.s3CredentialsFile = JSONX.getStrOrNull(obj, fS3CredsFile);
        if ( obj.hasKey(fS3CredsProfile) )
            conf.s3CredentialsProfile = JSONX.getStrOrNull(obj, fS3CredsProfile);
        if ( obj.hasKey(fS3Endpoint) )
            conf.s3Endpoint = JSONX.getStrOrNull(obj, fS3Endpoint);

        validate(conf);
        return conf;
    }

    public JsonObject asJSON() {
        return
            JSONX.buildObject(b->{
                if ( provider != null && provider != Provider.UNSET )
                    b.pair(fProvider, provider.name());

                if ( serverPort != null )
                    b.pair(fPort, serverPort.intValue());

                if ( jettyConf != null )
                    b.pair(fJetty, jettyConf);

                if ( zkConnectionString != null )
                    b.pair(fZkConnectionString, zkConnectionString);

                if ( zkPort != null )
                    b.pair(fZkPort, zkPort);

                if ( zkData != null )
                    b.pair(fZkData, zkData);

                if ( zkConf != null )
                    b.pair(fZkConfig, zkConf);

                if ( s3BucketName != null )
                    b.pair(fS3BucketName, s3BucketName);

                if ( s3Region != null )
                    b.pair(fS3Region, s3Region);

                if ( s3CredentialsFile != null )
                    b.pair(fS3CredsFile, s3CredentialsFile);

                if ( s3CredentialsProfile != null )
                    b.pair(fS3CredsProfile, s3CredentialsProfile);

                if ( s3Endpoint != null )
                    b.pair(fS3Endpoint, s3Endpoint);

                if ( fileBase != null )
                    b.pair(fFileDirData, fileBase);
            });
    }

    public static void writeJSON(DeltaServerConfig config, String file) {
        IOX.run(()->{
            JsonObject obj = config.asJSON();
            try (OutputStream out = Files.newOutputStream(Paths.get(file))) {
                JSON.write(out, obj);
        }});
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileBase == null) ? 0 : fileBase.hashCode());
        result = prime * result + ((jettyConf == null) ? 0 : jettyConf.hashCode());
        result = prime * result + ((provider == null) ? 0 : provider.hashCode());
        result = prime * result + ((s3BucketName == null) ? 0 : s3BucketName.hashCode());
        result = prime * result + ((s3CredentialsFile == null) ? 0 : s3CredentialsFile.hashCode());
        result = prime * result + ((s3CredentialsProfile == null) ? 0 : s3CredentialsProfile.hashCode());
        result = prime * result + ((s3Endpoint == null) ? 0 : s3Endpoint.hashCode());
        result = prime * result + ((s3Region == null) ? 0 : s3Region.hashCode());
        result = prime * result + ((serverPort == null) ? 0 : serverPort.hashCode());
        result = prime * result + ((zkConf == null) ? 0 : zkConf.hashCode());
        result = prime * result + ((zkConnectionString == null) ? 0 : zkConnectionString.hashCode());
        result = prime * result + ((zkData == null) ? 0 : zkData.hashCode());
        result = prime * result + ((zkMode == null) ? 0 : zkMode.hashCode());
        result = prime * result + ((zkPort == null) ? 0 : zkPort.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        DeltaServerConfig other = (DeltaServerConfig)obj;
        if ( fileBase == null ) {
            if ( other.fileBase != null )
                return false;
        } else if ( !fileBase.equals(other.fileBase) )
            return false;
        if ( jettyConf == null ) {
            if ( other.jettyConf != null )
                return false;
        } else if ( !jettyConf.equals(other.jettyConf) )
            return false;
        if ( provider != other.provider )
            return false;
        if ( s3BucketName == null ) {
            if ( other.s3BucketName != null )
                return false;
        } else if ( !s3BucketName.equals(other.s3BucketName) )
            return false;
        if ( s3CredentialsFile == null ) {
            if ( other.s3CredentialsFile != null )
                return false;
        } else if ( !s3CredentialsFile.equals(other.s3CredentialsFile) )
            return false;
        if ( s3CredentialsProfile == null ) {
            if ( other.s3CredentialsProfile != null )
                return false;
        } else if ( !s3CredentialsProfile.equals(other.s3CredentialsProfile) )
            return false;
        if ( s3Endpoint == null ) {
            if ( other.s3Endpoint != null )
                return false;
        } else if ( !s3Endpoint.equals(other.s3Endpoint) )
            return false;
        if ( s3Region == null ) {
            if ( other.s3Region != null )
                return false;
        } else if ( !s3Region.equals(other.s3Region) )
            return false;
        if ( serverPort == null ) {
            if ( other.serverPort != null )
                return false;
        } else if ( !serverPort.equals(other.serverPort) )
            return false;
        if ( zkConf == null ) {
            if ( other.zkConf != null )
                return false;
        } else if ( !zkConf.equals(other.zkConf) )
            return false;
        if ( zkConnectionString == null ) {
            if ( other.zkConnectionString != null )
                return false;
        } else if ( !zkConnectionString.equals(other.zkConnectionString) )
            return false;
        if ( zkData == null ) {
            if ( other.zkData != null )
                return false;
        } else if ( !zkData.equals(other.zkData) )
            return false;
        if ( zkMode != other.zkMode )
            return false;
        if ( zkPort == null ) {
            if ( other.zkPort != null )
                return false;
        } else if ( !zkPort.equals(other.zkPort) )
            return false;
        return true;
    }
}