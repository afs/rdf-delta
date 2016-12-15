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

package org.seaborne.delta;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.tdb.base.file.Location;
import org.junit.BeforeClass;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.DeltaLinkMgr;

public class TestLocalLink extends AbstractTestDeltaLink {
    @BeforeClass public static void setForTesting() { 
        //LogCtl.setLog4j();
        LogCtl.setJavaLogging();
    }
    
    private static boolean testForClass(String name) {
        try { 
            Class.forName(name);
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }
    
    // Set DataSource for local connection to look up.

    protected static Id dataId = Id.create();
    protected static DataRegistry dataRegistry = new DataRegistry("test");
    
    @Override
    public DeltaLink getLink() {
        DeltaLinkMgr linkMgr = new DeltaLinkMgr();
        return DeltaLinkLocal.create(dataRegistry, linkMgr);
    }

    @Override
    public void reset() {
        Location sourceArea = Location.mem(); 
        Location patchArea = Location.mem();
        DataSource dataSource = DataSource.attach(dataId, "uri", sourceArea, patchArea);
        dataRegistry.clear();
        dataRegistry.put(dataId, dataSource);
    }

    @Override
    public Id getDataSourceId() {
        return dataId;
    }

}
