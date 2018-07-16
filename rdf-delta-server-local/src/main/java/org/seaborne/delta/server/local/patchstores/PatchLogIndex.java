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

import java.util.function.Supplier;

import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.Version;
import org.seaborne.delta.server.local.PatchStore;

/** State control for a {@link PatchStore}. The index is {@code version -> id} mapping. */
public interface PatchLogIndex {
    
    /** Run action inside a patch log wide lock. */ 
    public void runWithLock(Runnable action);
    
    /** Run action inside a patch log wide lock; return a result. */
    public <X> X runWithLockRtn(Supplier<X> action);

    /** Return whether the log is empty. */ 
    public boolean isEmpty();
    
    /** 
     * Return the next version number.
     * Returns the same value until {@link #save(Version, Id, Id)} is called.  
     */
    public Version nextVersion();
    
    /** Save the new head of log information. */
    public void save(Version version, Id patch, Id prev);
    
    /**
     * Ensure the index is up-to-date.
     * This should not be necessary.
     */
    public void refresh();
    
    /**
     * Get the earliest version in the log.
     * Returns {@link DeltaConst#VERSION_INIT} when the log is empty.
     * Returns {@link DeltaConst#VERSION_UNSET} when the log has not been initialized yet.
     */
    public Version getEarliestVersion();

    /**
     * Get the {@code Id} of the earliest entry in the log or null if the log is empty.
     */
    public Id getEarliestId();
    
    /**
     * Get the {@code version} of the current head of the log.
     * Returns {@link DeltaConst#VERSION_INIT} when the log is empty.
     */
    
    public Version getCurrentVersion();
    
    /** Get the {@code Id} of the current head of the log, or null if there isn't one. */
    public Id getCurrentId();

    /** Get the {@code Id} of the previous entry, or null if there isn't one. */
    public Id getPreviousId();

    /** Map version number to the {@link Id} for the patch it refers to. */ 
    public Id mapVersionToId(Version version);

    /** Release the in-process state for this log index. */
    public void release();

    /** Delete (or make unavailable) the persistent state. */
    public void delete();

//    /** Map {@link Id} to version number. */ 
//    public long mapIdToVersion(Id id); 
}
