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

package org.seaborne.delta.link;

import java.util.List;
import java.util.function.Supplier;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.Version;
import org.seaborne.patch.RDFPatch ;

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
    public void ping() { exec(()->get().ping()); }

    @Override
    public void start() { exec(()->get().start()); }

    @Override
    public void close() { exec(()->get().close()); }

    @Override
    public void addListener(DeltaLinkListener listener) {
        other.addListener(listener);
    }

    @Override
    public void removeListener(DeltaLinkListener listener) {
        other.removeListener(listener);
    }
}
