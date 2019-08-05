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

package org.seaborne.delta.server.local.patchstores.file3;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;

public class PatchStoreProviderFile3{}
//
//public class PatchStoreProviderFile3 implements PatchStoreProvider {
//
//    public PatchStoreProviderFile3() {}
//
//    @Override
//    public PatchStore create(LocalServerConfig config) {
//        // The directory where all patch logs are kept.
//        String patchLogDirectory = config.getProperty(DeltaConst.pDeltaFile);
//        return create(patchLogDirectory);
//    }
//
//    //private static Map<String, PatchStoreFile2>
//
//    public PatchStoreFile2 create(String patchLogDirectory) {
//        if ( patchLogDirectory == null )
//            return null;
//        // [FILE2] Assumes a cache in PatchStoreFile2 of FilePatchIdx objects.
//        return new PatchStoreFile2(patchLogDirectory, this);
//    }
//
//    @Override
//    public String getProviderName() {
//        return DPS.PatchStoreFileProvider;
//    }
//
//    @Override
//    public String getShortName() {
//        return DPS.pspFile;
//    }
//
//    // For the file based PatchStore the index is an in-memory structure built from the
//    // FileStore to create the FilePatchIdx held in the PatchStoreFile2 object.
//
//    @Override
//    public PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
//        PatchStoreFile2 patchStoreFile = (PatchStoreFile2)patchStore;
//        FilePatchIdx filePatchIdex = patchStoreFile.getPatchLogFile(dsd.getId());
//        return new PatchLogIndexFile(filePatchIdex);
//    }
//
//    @Override
//    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
//        PatchStoreFile2 patchStoreFile = (PatchStoreFile2)patchStore;
//        FilePatchIdx filePatchIndex = patchStoreFile.getPatchLogFile(dsd.getId());
//        return new PatchStorageFile(filePatchIndex.fileStore(), filePatchIndex::idToVersion);
//    }
//}
