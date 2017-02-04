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

package dev.examples;

import java.io.IOException;
import java.util.List;

import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Id;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.delta.server.local.LocalServer;

/** Connect to a local server, create a new DataSource, remove it. */ 
public class Example1 {
    public static void example_local(String... args) throws IOException {
        Location loc = Location.create("DeltaServer");
        LocalServer server = LocalServer.attach(loc);
        state(server.listDataSources());
        System.out.println();
        
        // Correctly fails if exists
        Id newId = server.createDataSource(false, "XYZ", "http://example/xyz");
        DataSource dSrc = server.getDataRegistry().get(newId);
        
        List<DataSource> x = server.listDataSources();
        x.forEach((ds)->{
            System.out.println(ds);
            System.out.println("    "+ds.getURI());
            System.out.println("    "+ds.getId());
            System.out.println("    "+ds.getLocation());
            System.out.println("    "+ds.getPatchLog());
        });

        server.removeDataSource(newId);
        // Can not create again "remove" means "disable".
        System.out.println("DONE");
    }
    
    public static void state(List<DataSource> x) {
        x.forEach((ds)->{
            System.out.println(ds);
            System.out.println("    "+ds.getURI());
            System.out.println("    "+ds.getId());
            System.out.println("    "+ds.getLocation());
            System.out.println("    "+ds.getPatchLog());
        });
    }

}
