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

package org.seaborne.delta.server.local.patchstores.any;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreFile;
import org.seaborne.delta.server.local.patchstores.rdb.PatchStoreRocks;

/**
 * Base for "local" PatchStores - ones that use the file system.
 * <p>
 * In this situation, there can be a mixture of different PatchStore implements in the
 * same file area. {@link PatchStoreAnyLocal} provides the switching.
 * <p>
 * This case is the base for {@link PatchStoreAnyLocal "any"} as well as
 * the storage implementations {@link PatchStoreFile "file"} and
 * {@link PatchStoreRocks "RocksDB"}.
 * <p>
 */
public abstract class AbstractPatchStoreLocal extends PatchStore {

    protected final Path patchLogDirectory;

    protected AbstractPatchStoreLocal(String patchLogDirectory, PatchStoreProvider provider) {
        super(provider);
        Objects.requireNonNull(patchLogDirectory);
        this.patchLogDirectory = Paths.get(patchLogDirectory);
    }
}
