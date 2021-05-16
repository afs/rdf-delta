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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.systemtest.Matrix;

public class TestDeltaLogLockZk extends AbstractTestDeltaLogLock {

    static { TC_DeltaIntegration.setForTesting(); }

    private static DeltaServer deltaServer;
    private static DeltaLink dLink;

    @BeforeClass
    public static void before() {
        Matrix.setup();
        dLink = Matrix.deltaServerLink1;
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName("ABC");
        dsRef = ( dsd != null ) ? dsd.getId() : dLink.newDataSource("ABC", "http://data/ABC");
    }

    @AfterClass
    public static void after() {
        Matrix.teardown();
    }

    @Override
    protected DeltaLink getDLink() {
        return dLink;
    }
}
