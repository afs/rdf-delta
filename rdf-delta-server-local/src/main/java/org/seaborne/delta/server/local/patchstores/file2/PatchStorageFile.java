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

package org.seaborne.delta.server.local.patchstores.file2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DeltaNotFoundException;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.IOX.IOConsumer;
import org.seaborne.delta.server.local.patchstores.PatchStorage;
import org.seaborne.delta.server.local.patchstores.filestore.FileEntry;
import org.seaborne.delta.server.local.patchstores.filestore.FileStore;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesWriter;
import org.seaborne.patch.text.RDFPatchReaderText;
import org.seaborne.patch.text.TokenWriter;
import org.seaborne.patch.text.TokenWriterText;

/** Storage of patches using files. */
public class PatchStorageFile implements PatchStorage {

    private final Function<Id, Version> id2version;
    private final FileStore fileStore;

    public PatchStorageFile(FileStore fileStore, Function<Id, Version> id2version) {
        // File-based patch storage is by version number, hence "id2version" required.
        this.fileStore = fileStore;
        this.id2version = id2version;
    }

    @Override
    public Stream<Id> find() {
        // Reads the disk to find the ids. For testing.
        Iterator<Long> iter = fileStore.scanIndex().iterator();
        List<Id> ids = new ArrayList<>();
        iter.forEachRemaining(v->{
            try ( InputStream in = fileStore.open(v) ) {
                PatchHeader patchHeader = RDFPatchReaderText.readerHeader(in);
                Node n = patchHeader.getId();
                ids.add(Id.fromNode(n));
            } catch (IOException ex) {
                throw IOX.exception(ex);
            }
        });
        return ids.stream();
    }

    @Override
    public void store(Id key, RDFPatch patch) {
        throw new InternalErrorException("Call to PatchStorageFile.store(id, patch)");
    }

    @Override
    public void store(Version version, Id key, RDFPatch patch) {
        IOConsumer<OutputStream> action = out -> {
            TokenWriter tw = new TokenWriterText(out) ;
            RDFChangesWriter dest = new RDFChangesWriter(tw) ;
            patch.apply(dest);
        };

        FileEntry file = fileStore.allocateFilename(version.value());
        file.write(action);
        fileStore.completeWrite(file);
    }

    private Version idToVersion(Id id) {
        return id2version.apply(id);
    }

    @Override
    public RDFPatch fetch(Id id) {
        Version ver = idToVersion(id);
        try ( InputStream in = fileStore.open(ver.value()) ) {
            RDFPatch patch = RDFPatchOps.read(in) ;
            return patch;
        }
        catch ( DeltaNotFoundException ex)  // Our internal 404.
        { return null; }
        catch (IOException ex) {
            throw IOX.exception(ex);
        }
    }

    @Override
    public void delete(Id id) {
        Version ver = idToVersion(id);
        Path p = fileStore.filename(ver.value());
        try {
            Files.delete(p);
        } catch (IOException ex) {
            throw IOX.exception(ex);
        }
    }
}
