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

import static org.junit.Assert.assertEquals ;
import static org.junit.Assert.assertNotNull ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.tdb.base.file.Location ;
import org.junit.AfterClass ;
import org.junit.BeforeClass ;
import org.junit.Test ;
import org.seaborne.delta.client.LocalStorageType ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.client.DeltaConnection ;
import org.seaborne.delta.client.Zone ;
import org.seaborne.delta.link.DeltaLink ;
import org.seaborne.delta.link.RegToken ;

public abstract class AbstractTestDeltaClient {

    @BeforeClass public static void setupZone() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
        String DIR = "target/Zone";
        Location loc = Location.create(DIR);
        FileOps.ensureDir(DIR);
        Zone.get().init(loc);
    }
    
    @AfterClass public static void cleanOutZone() {
        Zone.get().shutdown();
    }
    
    protected abstract Setup.LinkSetup getSetup();
    
    protected DeltaLink getLink() {
        DeltaLink dLink = getSetup().getLink() ;
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        return dLink;
    }

    protected DeltaLink getLinkUnregister() {
        return getSetup().getLink() ;
    }    
    
    protected Zone getZone() { return Zone.get(); }
    
    protected DeltaClient createDeltaClient() {
        return DeltaClient.create(getZone(), getLink());  
    }
    
    @Test
    public void create_1() {
        // Create on the Delta link then setup DeltaClient
        DeltaLink dLink = getLink();
        String DS_NAME = "123";
        
        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource");
        DeltaClient dClient = createDeltaClient();
        dClient.attach(dsRef, LocalStorageType.EXTERNAL);
        
        DeltaConnection dConn = dClient.get(DS_NAME);
        assertNotNull(dConn);
        assertEquals(0, dConn.getLocalVersion());
        assertEquals(0, dConn.getRemoteVersionLatest());
    }


    @Test
    public void create_datasource_1() {
        // Create via DeltaClient
        String DS_NAME = "1234";
        DeltaClient dClient = createDeltaClient();
        Id dsRef1 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME, LocalStorageType.MEM);
    }

    @Test(expected=DeltaBadRequestException.class)
    public void create_datasource_2() {
        String DS_NAME = "123456";
        DeltaClient dClient = createDeltaClient();
        Id dsRef1 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME, LocalStorageType.MEM);
        // Error.
        Id dsRef2 = dClient.newDataSource(DS_NAME, "http://example/"+DS_NAME, LocalStorageType.MEM);
    }
    
    @Test(expected=DeltaBadRequestException.class)
    public void attach_non_existing() {
        DeltaLink dLink = getLink();
        Id dsRef = Id.create();
        DeltaClient dClient = createDeltaClient();
        dClient.attach(dsRef, LocalStorageType.MEM);
    }
    
    @Test
    public void local_storage_1() {}
    
    //TODO
    //  test local storage
    //  test restart
    
    // test delete , local and system

}
