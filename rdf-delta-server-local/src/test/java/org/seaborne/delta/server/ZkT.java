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

package org.seaborne.delta.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.test.TestingServer;

/** Zookeeper testing support */
public class ZkT {
    
    public static List<TestingServer> servers = new ArrayList<>(); 
    
    /** Stop all {@code TestingServers}. */ 
    public static void clearAll() {
        servers.forEach(s->{
            try {
                s.stop();
            } catch (Exception ex) {}
        });
    }
    
    /** Create a testing ZooKeeper server: record in {@link ZkT#servers}. */
    public static TestingServer localServer() {
        try {
            TestingServer server = new TestingServer();
            servers.add(server);
            server.start();
            return server ;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
