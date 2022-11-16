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

package org.seaborne.delta.link;

import java.util.List;
import java.util.function.Supplier;

import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.*;
import org.apache.jena.rdfpatch.RDFPatch ;

/** Wrapper for {@link DeltaLink} which can be subclassed to provide
 *  the other {@code DeltaLink} dynamically (see protected operation {@link #get})
 *  and have retry strategies (see protected operations {@link #exec} and {@link #execRtn}).
 *
 */
public class DeltaLinkWrapper implements DeltaLink {

    private DeltaLink other;

    public DeltaLinkWrapper(DeltaLink other) {
        this.other = other;
    }

    // Can override for other polices,
    protected DeltaLink get() { return other; }

    // Execution policies.
    // Note: DeltalLink operations are "retryable"
    protected <T> T execRtn(Supplier<T> action) { return action.get(); }
    protected void exec(Runnable action) { action.run(); }

    @Override
    public Id newDataSource(String name, String uri) {
        return execRtn(()->get().newDataSource(name, uri));
    }

    @Override
    public void removeDataSource(Id dsRef) {
        exec(()->get().removeDataSource(dsRef));
    }

    @Override
    public Id copyDataSource(Id id, String oldName, String newName) {
        return execRtn(()->get().copyDataSource(id, oldName, newName));
    }

    @Override
    public Id renameDataSource(Id id, String oldName, String newName) {
        return execRtn(()->get().renameDataSource(id, oldName, newName));
    }

    @Override
    public List<Id> listDatasets() {
        return execRtn(()->get().listDatasets());
    }

    @Override
    public List<PatchLogInfo> listPatchLogInfo() {
        return execRtn(()->get().listPatchLogInfo());
    }

    @Override
    public PatchLogInfo getPatchLogInfo(Id dsRef) {
        return execRtn(()->get().getPatchLogInfo(dsRef));
    }

    @Override
    public List<DataSourceDescription> listDescriptions() {
        return execRtn(()->get().listDescriptions());
    }

    @Override
    public DataSourceDescription getDataSourceDescription(Id dsRef) {
        return execRtn(()->get().getDataSourceDescription(dsRef));
    }

    @Override
    public DataSourceDescription getDataSourceDescriptionByURI(String uri) {
        return execRtn(()->get().getDataSourceDescriptionByURI(uri));
    }

    @Override
    public DataSourceDescription getDataSourceDescriptionByName(String name) {
        return execRtn(()->get().getDataSourceDescriptionByName(name));
    }

    @Override
    public Version append(Id dsRef, RDFPatch patch) {
        return execRtn(()->get().append(dsRef, patch));
    }

    @Override
    public RDFPatch fetch(Id dsRef, Version version) {
        return execRtn(()->get().fetch(dsRef, version));
    }

    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        return execRtn(()->get().fetch(dsRef, patchId));
    }

    @Override
    public String initialState(Id dsRef) {
        return execRtn(()->get().initialState(dsRef));
    }

    @Override
    public void addListener(DeltaLinkListener listener) {
        other.addListener(listener);
    }

    @Override
    public void removeListener(DeltaLinkListener listener) {
        other.removeListener(listener);
    }

    @Override
    public JsonObject ping() { return execRtn(()->get().ping()); }

    @Override
    public void start() { exec(()->get().start()); }

    @Override
    public void close() { exec(()->get().close()); }

    @Override
    public Id acquireLock(Id datasourceId) {
        return execRtn(() -> get().acquireLock(datasourceId));
    }

    @Override
    public boolean refreshLock(Id datasourceId, Id lockRef) {
        return execRtn(()->get().refreshLock(datasourceId, lockRef));
    }

//    @Override
//    public Set<Id> refreshLocks(Set<Id> lockSet) {
//        return execRtn(()->get().refreshLocks(lockSet));
//    }

    @Override
    public LockState readLock(Id datasourceId) {
        return execRtn(()->get().readLock(datasourceId));
    }

    @Override
    public Id grabLock(Id datasourceId, Id oldSession) {
        return execRtn(()->get().grabLock(datasourceId, oldSession));
    }

    @Override
    public void releaseLock(Id datasourceId, Id session) {
        exec(() -> get().releaseLock(datasourceId, session));
    }
}
