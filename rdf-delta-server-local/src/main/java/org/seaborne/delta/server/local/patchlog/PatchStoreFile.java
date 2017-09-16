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

package org.seaborne.delta.server.local.patchlog;

import java.nio.file.Path ;

import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.server.local.DPS ;

//PatchStoreFileProvider
public class PatchStoreFile extends PatchStore {

    public static void registerPatchStoreFile() {
        PatchStore ps = new PatchStoreFile();
        PatchStore.register(ps);
    }
    
    public PatchStoreFile() {
        super(DPS.PatchStoreFileProvider) ;
    }

    @Override
    protected PatchLogFile create(DataSourceDescription dsd, Path logPath) {
        Location loc = Location.create(logPath.toString());
        PatchLogFile patchLog = PatchLogFile.attach(dsd, loc);
        return patchLog ;
    }
}
