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

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.patch.RDFPatch;

public abstract class AbstractPatchLog implements PatchLog {

    private final DataSourceDescription dsd;

    protected abstract RDFPatch earliestPatch();
    protected abstract RDFPatch latestPatch();
    protected abstract long earliestVersion();
    protected abstract long latestVersion();
    
    protected AbstractPatchLog(DataSourceDescription dsd) {
        this.dsd = dsd;
    }

    @Override
    public Id getEarliestId() {
        RDFPatch p = earliestPatch();
        return p == null ? null : Id.fromNode(p.getId());
    }

    @Override
    public long getEarliestVersion() {
        return earliestVersion();
    }

    @Override
    public Id getLatestId() {
        RDFPatch p = latestPatch();
        return p == null ? null : Id.fromNode(p.getId());
    }

    @Override
    public long getLatestVersion() {
        return latestVersion();
    }

    @Override
    public PatchLogInfo getDescription() {
        return new PatchLogInfo(dsd, getEarliestVersion(), getLatestVersion(), getLatestId());
    }

    @Override
    public Id getLogId() {
        return dsd.getId();
    }

    @Override
    public boolean isEmpty() {
        return getEarliestVersion() < 0;
    }

//    @Override
//    public boolean contains(Id patchId) {
//        return false;
//    }
//
//    @Override
//    public long append(RDFPatch patch) {
//        return 0;
//    }
//
//    @Override
//    public RDFPatch fetch(Id patchId) {
//        return null;
//    }
//
//    @Override
//    public RDFPatch fetch(long version) {
//        return null;
//    }
//
//    @Override
//    public Stream<RDFPatch> range(Id start, Id finish) {
//        return null;
//    }
//
//    @Override
//    public Stream<RDFPatch> range(long start, long finish) {
//        return null;
//    }
//
//    @Override
//    public Id find(long version) {
//        return null;
//    }
//
//    @Override
//    public long find(Id id) {
//        return 0;
//    }
//
//    @Override
//    public void release() {}
}
