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

import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;

public class Setup {
    interface LinkSetup {
        public void beforeClass();
        public void afterClass();
        public void beforeTest();
        public void afterTest();
        public DeltaLink getLink();
    }
    
    public static class LocalSetup implements LinkSetup {
        protected LocalServer lserver = null;
        protected DeltaLink dlink = null;

        @Override
        public void beforeClass() {}

        @Override
        public void afterClass() {}

        @Override
        public DeltaLink getLink() {
            return dlink;
        }

        @Override
        public void beforeTest() {
            lserver = DeltaTestLib.createEmptyTestServer();
            dlink =  DeltaLinkLocal.create(lserver);
        }
        
        @Override
        public void afterTest() {
            if ( lserver != null )
                LocalServer.release(lserver);
        }
    }

    public static class RemoteSetup implements LinkSetup {

        @Override
        public void beforeClass() {}

        @Override
        public void afterClass() {}

        @Override
        public void beforeTest() {}

        @Override
        public void afterTest() {}

        @Override
        public DeltaLink getLink() {
            return null;
        }

    }
}
