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

package org.seaborne.delta.server.local.patchstores;

import java.util.stream.Stream;

import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.LogEntry;

public interface LogIndex {

    /** Add a {@link LogEntry}.
     * <p>
     * The calling code guarantees that the entry values are correct - new version is the
     * next version to use, id is new, and previous refers to an existing log entry.
     * <p>
     * It is called inside the {@link PatchLogIndex} lock.
     */
    public void save(Version version, Id id, Id previous);

    /** Stream of all entries, in no particular order.
     *
     * This operation is not used for normal operation but may be used for administration
     * tasks. It must be reliable in the presence of concurrent requests, but can return
     * an inconsistent view of the index.
     */
    public Stream<LogEntry> entries();

    /** Map version to id.
     * <p>
     * Return return null if the argument is null.
     */
    public Id versionToId(Version version);

    /** Return the next version.
     *
     * This must return the same value each time until the call of {@link #save}
     * after which is must return a different values to before the call.
     */
    public Version genNextVersion();

    /** Get the {@link LogEntry} for this id.
     * <p>
     * Return return null if the argument is null.
     */
    public LogEntry getPatchInfo(Id id);

    /** Earliest version in the index.
     *
     * When there are no versions, returns null.
     * Once there are any versions, this is then fixed (subject to index reorganisation).
     */
    public Version earliest();

    /** Current latest version in the index. */
    public Version current();
}
