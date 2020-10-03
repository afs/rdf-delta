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

import java.util.concurrent.atomic.LongAdder;

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.patch.RDFPatch;

/**
 * {@link DeltaLink} listener that counts operations.
 */
public class DeltaLinkCounter implements DeltaLinkListener {
    private LongAdder cNew = new LongAdder();
    private LongAdder cCopied = new LongAdder();
    private LongAdder cRename = new LongAdder();
    private LongAdder cRemove = new LongAdder();
    private LongAdder cFetch = new LongAdder();
    private LongAdder cAppend = new LongAdder();

    public long countCreated()  { return cNew.longValue(); }
    public long countCopied()   { return cCopied.longValue(); }
    public long countRenamed()  { return cRename.longValue(); }
    public long countRemoved()  { return cRemove.longValue(); }
    public long countFetch()    { return cFetch.longValue(); }
    public long countAppend()   { return cAppend.longValue(); }

    @Override
    public void newDataSource(Id dsRef, String name) {
        cNew.increment();
    }

    @Override
    public void copyDataSource(Id dsRef, Id dsRef2, String oldName, String newName) {
        cCopied.increment();
    }

    @Override
    public void renameDataSource(Id dsRef, Id dsRef2, String oldName, String newName) {
        cRename.increment();
    }

    @Override
    public void removeDataSource(Id dsRef) {
        cRemove.increment();
    }

    @Override
    public void fetchById(Id dsRef, Id patchId, RDFPatch patch) {
        cFetch.increment();
    }

    @Override
    public void fetchByVersion(Id dsRef, Version version, RDFPatch patch) {
        cFetch.increment();
    }

    @Override
    public void append(Id dsRef, Version version, RDFPatch patch) {
        cAppend.increment();
    }
}
