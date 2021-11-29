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

package org.seaborne.delta;

import static org.junit.Assert.assertEquals ;
import static org.junit.Assert.assertNotNull ;

import org.apache.jena.atlas.iterator.Iter ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.base.file.Location ;
import org.junit.AfterClass ;
import org.junit.BeforeClass ;
import org.junit.Test ;
import org.seaborne.delta.client.*;
import org.seaborne.delta.lib.LogX;
import org.seaborne.delta.link.DeltaLink ;

public abstract class AbstractTestDeltaClient {
    // See also with AbstractTestDeltaConnection

    static String DIR_ZONE = "target/Zone";
    static protected Zone zone;

    @BeforeClass public static void setupZone() {
        LogX.setJavaLogging("src/test/resources/logging.properties");
        Location loc = Location.create(DIR_ZONE);
        FileOps.ensureDir(DIR_ZONE);
        FileOps.clearAll(DIR_ZONE);
        zone = Zone.connect(DIR_ZONE);
    }

    @AfterClass public static void cleanOutZone() {
        zone.shutdown();
    }

    protected abstract Setup.LinkSetup getSetup();

    protected DeltaLink getLink() {
        return getSetup().getLink() ;
    }

    protected DeltaClient createDeltaClient() {
        return DeltaClient.create(zone, getLink());
    }

    @Test
    public void create_1() {
        // Create on the Delta link then setup DeltaClient
        DeltaLink dLink = getLink();
        String DS_NAME = "123";

        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource");
        DeltaClient dClient = createDeltaClient();

        //dClient.register(dsRef, LocalStorageType.EXTERNAL, TxnSyncPolicy.NONE);
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);

        DeltaConnection dConn = dClient.get(DS_NAME);
        assertNotNull(dConn);
        assertEquals(Version.INIT, dConn.getLocalVersion());
        assertEquals(Version.INIT, dConn.getRemoteVersionLatest());
    }

    @Test
    public void create_datasource_1() {
        // Create via DeltaClient
        String DS_NAME = "1234";
        DeltaClient dClient = createDeltaClient();
        Id dsRef1 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
    }

    @Test(expected=DeltaBadRequestException.class)
    public void create_datasource_2() {
        String DS_NAME = "123456";
        DeltaClient dClient = createDeltaClient();
        Id dsRef1 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
        // Error.
        Id dsRef2 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME);
    }

    @Test(expected=DeltaBadRequestException.class)
    public void attach_non_existing() {
        DeltaLink dLink = getLink();
        Id dsRef = Id.create();
        DeltaClient dClient = createDeltaClient();
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
    }

    @Test
    public void update_1() {
        // Create on the Delta link then setup DeltaClient
        DeltaLink dLink = getLink();
        String DS_NAME = "123";

        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource_update_1");
        DeltaClient dClient = createDeltaClient();
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
        DeltaConnection dConn = dClient.get(DS_NAME);
        assertNotNull(dConn);
        assertEquals(Version.INIT, dConn.getLocalVersion());
        assertEquals(Version.INIT, dConn.getRemoteVersionLatest());

        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        DatasetGraph dsg = dConn.getDatasetGraph();
        long x0 = Iter.count(dsg.find());
        assertEquals(0, x0);
        Txn.executeWrite(dsg, ()->dsg.add(quad));

        long x1 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()));
        assertEquals(1, x1);

        DatasetGraph dsgx = dConn.getStorage();
        long x2 = Txn.calculateRead(dsgx, ()->Iter.count(dsgx.find()));
        assertEquals(1, x1);

    }

    @Test
    public void update_2() {
        // Create on the Delta link then setup DeltaClient
        DeltaLink dLink = getLink();
        String DS_NAME = "1234";

        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource_update_2");
        DeltaClient dClient = createDeltaClient();
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
        DeltaConnection dConn = dClient.get(DS_NAME);
        assertNotNull(dConn);
        assertEquals(Version.INIT, dConn.getLocalVersion());
        assertEquals(Version.INIT, dConn.getRemoteVersionLatest());

        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        DatasetGraph dsg = dConn.getDatasetGraph();

        long x0 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()) );
        assertEquals(0, x0);

        Txn.executeWrite(dsg, ()->dsg.add(quad));
        long x1 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()) );
        assertEquals(1, x1);

        long x2 = Iter.count(dConn.getStorage().find());
        assertEquals(1, x1);
    }

    @Test
    public void update_3() {
        // Create on the Delta link then setup DeltaClient
        DeltaLink dLink = getLink();
        String DS_NAME = "12345";

        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource_update_3");
        DeltaClient dClient = createDeltaClient();
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
        DeltaConnection dConn = dClient.get(DS_NAME);
        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        DatasetGraph dsg = dConn.getDatasetGraph();

        long x0 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()) );
        assertEquals(0, x0);

        dsg.begin(ReadWrite.WRITE);
        dsg.add(quad);
        dsg.abort();

        long x1 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()) );
        assertEquals(0, x1);
    }


    @Test
    public void local_storage_1() {
        String DS_NAME = "654321";
        testLocalStorage(LocalStorageType.TDB, DS_NAME);
    }

    @Test
    public void local_storage_2() {
        String DS_NAME = "6543";
        testLocalStorage(LocalStorageType.TDB2, DS_NAME);
    }

    private void testLocalStorage(LocalStorageType localStorageType, String dsName) {
        DeltaClient dClient = createDeltaClient();
        Id dsRef = dClient.newDataSource(dsName, "http://example/"+dsName);
        dClient.register(dsRef, localStorageType, SyncPolicy.NONE);

        Quad quad = SSE.parseQuad("(_ :s :p :o)");

        try(DeltaConnection dConn = dClient.get(dsRef)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad));
        }

        // Reset Zone.
        Location loc = zone.getLocation();
        zone.shutdown();
        zone = Zone.connect(loc);
        // Reset.
        dClient = DeltaClient.create(zone, getLink());
        dClient.connect(dsRef, SyncPolicy.NONE);

        try(DeltaConnection dConn = dClient.get(dsRef)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            long x0 = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()) );
            assertEquals(1, x0);
        }
    }

    //TODO
    //  test local storage
    //  test restart

    // test delete , local and system

}
