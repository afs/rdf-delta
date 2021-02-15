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

package org.seaborne.delta.cmds;

import static org.junit.Assert.assertEquals;

import delta.server.DeltaServerCmd;
import delta.server.DeltaServerConfig;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.cmd.CmdException;
import org.junit.Test;

public class TestDeltaServerConfig {

    @Test public void server_config_mem_1() {
        test("--mem");
    }

    @Test public void server_config_mem_2() {
        test("--port=1064", "--mem");
    }

    @Test public void server_config_mem_3() {
        test("--jetty=jetty.xml", "--mem");
    }

    @Test public void server_config_zk_1() {
        test("--zk=mem");
    }

    @Test public void server_config_zk_2() {
        test("--zk=host1:1001,host2:1002,host3:1003");
    }

    @Test public void server_config_zk_3() {
        test("--zk=localhost:9909", "--zkPort=9909", "--zkData=DIR");
    }

    @Test public void server_config_zk_4() {
        test("--zk=localhost:9909", "--zkConf=zoo.cfg");
    }

    @Test(expected=CmdException.class)
    public void server_config_zks3_1() {
        test("--zk=mem", "--s3bucket=patches");
    }

    @Test public void server_config_zks3_2() {
        test("--zk=mem", "--s3bucket=patches", "--s3region=eu-bristol");
    }

    @Test public void server_config_zks3_3() {
        test("--zk=mem", "--s3bucket=patches", "--s3region=eu-bristol", "--s3keys=credentials");
    }

    @Test public void server_config_file_1() {
        test("--base=target");
    }

    @Test(expected=RuntimeException.class)
    public void server_config_bad_01() {
        test("--mem", "--base=DIR");
    }

    @Test(expected=RuntimeException.class)
    public void server_config_bad_02() {
        test("--mem", "--zk=mem");
    }

    @Test(expected=RuntimeException.class)
    public void server_config_bad_zk_01() {
        test("--zkConf=zoo.cfg");
    }

    @Test(expected=RuntimeException.class)
    public void server_config_bad_zk_02() {
        test("--zk=localhost:99" ,"--zkPort=99");
    }

    @Test(expected=RuntimeException.class)
    public void server_config_bad_03() {
        test("--zk=localhost:99" ,"--zkData=DIR");
    }

    private DeltaServerConfig test(String...args) {
        DeltaServerConfig c = DeltaServerCmd.processArgs(args);
        roundTrip(c);
        return c;
    }

    private void roundTrip(DeltaServerConfig c) {
        JsonObject obj = c.asJSON();
        DeltaServerConfig c2 = DeltaServerConfig.create(obj);
        if ( ! c.equals(c2) ) {
            System.out.println("c  : "+c.zkMode);
            System.out.println("c2 : "+c2.zkMode);
            JSON.write(obj);
            JSON.write(c2.asJSON());
        }
        assertEquals(c, c2);
    }
}
