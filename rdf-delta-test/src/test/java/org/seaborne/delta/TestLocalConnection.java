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
import org.junit.BeforeClass;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
import org.seaborne.delta.server.local.*;

public class TestLocalConnection extends AbstractTestDeltaConnection {
    static { LogCtl.setJavaLogging(); }
    
    // DRY - TestLocalLink
    @BeforeClass public static void setForTesting() { 
    }

    protected Id dataId = Id.create();
    protected DataRegistry dataRegistry = new DataRegistry("test");
    
    @Override
    public DeltaLink getLink() {
        DeltaLinkMgr linkMgr = new DeltaLinkMgr();
        return DeltaLinkLocal.create(dataRegistry, linkMgr);
    }

    @Override
    public void reset() {
        FileStore.resetTracked();
        DeltaTestLib.resetTestAreas();
        DataSource dataSource = DataSource.attach(dataId, "uri", DeltaTestLib.SourceArea, DeltaTestLib.PatchArea);
        dataRegistry.clear();
        dataRegistry.put(dataId, dataSource);
        int x = dataSource.getPatchSet().getFileStore().getCurrentIndex();
    }

    @Override
    public Id getDataSourceId() {
        return dataId;
    }
}
