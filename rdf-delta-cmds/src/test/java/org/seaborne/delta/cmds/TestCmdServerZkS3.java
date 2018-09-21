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

import java.io.PrintStream;
import java.net.BindException;
import java.util.function.Consumer;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

import delta.server.DeltaServer;
import io.findify.s3mock.S3Mock;
import org.apache.jena.atlas.io.NullOutputStream;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.riot.web.HttpOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.server.http.PatchLogServer;

/** Test running the server command with an in-memory Zk and S3 mock up */
public class TestCmdServerZkS3 {
    private final static String REGION = "uk-bristol-1";
    private S3Mock api;
    private String endpointURL;

    @Before public void before() {
        int port = LibX.choosePort();
        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:"+port, REGION);
        api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        api.start();
        endpointURL = endpoint.getServiceEndpoint();
    }

    @After public void after() {
        api.shutdown();
        endpointURL = null;
    }

    @Test public void deltaZkS3_1() {
        runDevNull(()->
        runtest(
            (serverEp)->delta.dcmd.main("ls", "--server="+serverEp, "ACE"),
            endpointURL)
        );
    }

    @Test public void deltaZkS3_2() {
        runtest(
            (endpoint)->HttpOp.execHttpGet(endpoint+"$/ping"),
            endpointURL);
    }

    @Test(expected=HttpException.class)
    public void deltaZkS3_3() {
        runtest(
            (endpoint)-> {
                // Would cause a 404 log message.
                // Hidden by the logging configuration.
                HttpOp.execHttpGet(endpoint+"$/noSuch");
            },
            endpointURL);
    }

    @Test public void deltaZkS3_4() throws Exception {
        runDevNull(()->
            runtest(
                (serverEp)->{
                    delta.dcmd.main("mk",  "--server="+serverEp,  "ACE");
                    delta.dcmd.main("add", "--server="+serverEp,  "--log=ACE", "testing/data.rdfp");
                    delta.dcmd.main("ls",  "--server="+serverEp,  "ACE");
                }, endpointURL)
            );
    }

    // Run sending System.out to /dev/null
    // Note: this is unliely to affect logging - System.out wil have been read and stored internally.
    private static void runDevNull(Runnable action) {
        PrintStream nullOutput = new PrintStream(new NullOutputStream());
        PrintStream ps = System.out;
        System.setOut(nullOutput);
        try {
            action.run();
        } finally {
            System.setOut(ps);
        }
    }

    private static void runtest(Consumer<String> action,  String s3EndpointURL) {
        int port = LibX.choosePort();
        // Imperfect port choice but in practice pretty good.
        int zkPort = LibX.choosePort();

        PatchLogServer server;
        try {
            server =
                DeltaServer.build(
                    "--port="+port,
                    "--zk=mem",
                    "--zkPort="+zkPort,
                    "--s3bucket=afs-delta-1",
                    "--s3region="+REGION,
                    "--s3endpoint="+s3EndpointURL
                    )
                .start();
        } catch (BindException ex) {
            throw new RuntimeException(ex);
        }

        int serverPort = server.getPort();
        String deltaEndpoint = "http://localhost:"+serverPort+"/";

        try {
            action.accept(deltaEndpoint);
        }
        finally {
            server.stop();
        }
    }
}
