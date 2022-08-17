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

import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.*;
import org.seaborne.patch.RDFPatch;

/**
 * Interface to the server for the operations.
 * <p>
 * A server is any engine that that provides the Delta operations and it may be local or
 * remote.
 * <p>
 * The client connection for operations on a specific dataset is {@code DeltaConnection}.
 */
public interface DeltaLink {
    /**
     * Create a new dataset and return the unique Id for it.
     * <p>
     * The {@code name} is an unused name for this link; it is a file name (not a path).
     * <p>
     * The {@code uri} is carried around with the data source.
     */
    public Id newDataSource(String name, String uri);

    /** Copy patch log.
     * <p>
     * The new log has the given new name, and URI modified from the old patch log.
     * <p>
     * Optional operation - may not be supported, or may not, be supported in all cases
     * (e.g. a server may only allow rename within the same storage provider).
     */
    public Id copyDataSource(Id dsRef, String srcName, String dstName);

    /** Change the short name for a patch log.
     * <p>
     * The {@code uri} and {@code Id} of the data source is not changed,
     * only the short name.
     * <p>
     * Optional operation - may not be supported, or may not, be supported in all cases
     * (e.g. a server may only allow rename within the same storage provider).
     */
    public Id renameDataSource(Id dsRef, String oldName, String newName);

    /** Make a dataset unavailable.
     *  Actual deleting of resources depends on the implementation.
     *  (A server will tend not to really delete a persistent database.)
     */
    public void removeDataSource(Id dsRef);

    /** Return an array of ids of datasets */
    public List<Id> listDatasets();

    /** Return details of all patch logs.
     * <p>
     * This is the operation to use to poll a patch log server for detailed information
     * about the current patch logs that exist and their current state.
     */
    public List<PatchLogInfo> listPatchLogInfo();

    /** Return details of the patch log, or null if not registered. */
    public PatchLogInfo getPatchLogInfo(Id dsRef);

    /** Return an array of {@link DataSourceDescription}s of datasets */
    public List<DataSourceDescription> listDescriptions();

    /** Return details of a patch log, or null if not registered. */
    public DataSourceDescription getDataSourceDescription(Id dsRef);

    /**
     * Test whether patch log exists or not.
     * Calling {@link #getDataSourceDescription} to get details,or null, is better than
     * calling {@code exists} if the description is needed.
     */
    public default boolean exists(Id dsRef) {
        return getDataSourceDescription(dsRef) != null;
    }

    /**
     * Return a {@link DeltaLog} object for the specificed patch log.
     * This does not guarantee the log exists - use {@link #exists} to check
     * if required.
     */
    public default DeltaLog getDeltaLog(Id dsRef) {
        return new DeltaLog(this, dsRef);
    }

    /** Return the name of a dataset, or {@literal <null>} if not registered. */
    public default String getDataSourceName(Id dsRef) {
        DataSourceDescription dsd = getDataSourceDescription(dsRef);
        if ( dsd == null )
            return "<null>";
        return dsd.getName();
    }

    /** Return details of a dataset, or null if not registered. */
    public DataSourceDescription getDataSourceDescriptionByURI(String uri);

    /**
     * Test whether patch log exists or not.
     * Calling {@link #getDataSourceDescriptionByURI} to get details,or null, is better than
     * calling {@code existsByURI} if the description is needed.
     */
    public default boolean existsByURI(String uri) {
        return getDataSourceDescriptionByURI(uri) != null;
    }


    /** Return details of a dataset, or null if not registered. */
    public DataSourceDescription getDataSourceDescriptionByName(String name);

    /**
     * Test whether patch log exists or not.
     * Calling {@link #getDataSourceDescriptionByName} to get details,or null, is better than
     * calling {@code existsByName} if the description is needed.
     */
    public default boolean existsByName(String name) {
        return getDataSourceDescriptionByName(name) != null;
    }

    /** Send patch, return new version.
     *  Return -1 for the patch didn't happen, and there is no change
     *  to the log.
     *  <p>
     *  For example, it wasn't tried remotely
     *  because it was suppressed for some reason.
     *  (e.g. empty commit suppression).
     */
    public Version append(Id dsRef, RDFPatch patch);

    /** Get the current version: if this is an HTTP connection, this causes network traffic. */
    public default Version getCurrentVersion(Id dsRef) { return getPatchLogInfo(dsRef).getMaxVersion(); }

    /** Retrieve a patch by data source and version. */
    public RDFPatch fetch(Id dsRef, Version version);

    /** Retrieve a patch by data source and patch id. */
    public RDFPatch fetch(Id dsRef, Id patchId);

    /**
     * Retrieve a URL to the initial state.
     * The log starts with this state.
     */
    public String initialState(Id dsRef);

    /** Add a {@link DeltaLinkListener} listener. */
    public void addListener(DeltaLinkListener listener);

    /** Remove a {@link DeltaLinkListener} listener. */
    public void removeListener(DeltaLinkListener listener);

    /** No-op end-to-end operation. This operation succeeds or throws an exception.
     *  This operation makes one attempt only to perform the ping even if the {@code DeltaLink}
     *  has some level of retry policy.
     */
    public JsonObject ping();

    /** Start the link. */
    public void start();

    /** Shutdown the link. */
    public void close();

    // Get Lock?
    // Lock = owner + timestamp (owner clock)

    /**
     * Acquire the lock for a data source.
     * Data source locks are not reentrant.
     * This operation is blocking.
     *
     * Returns an {@link Id} for the lock ownership.<br/>
     * Returns null for failure to get the lock.
     */
    public Id acquireLock(Id datasourceId);

    /**
     * Refresh a set of locks. This operation is blocking.
     *
     * Return true if the lock is still valid, false if not (lock has been released or has
     * timed-out).
     */
    public boolean refreshLock(Id datasourceId, Id lockSession);

    /**
     * Read the details of lock: the session/ownership id and the refresh index.
     *
     * Return true if the lock is still valid; return null if the lock is no longer held.
     */
    public LockState readLock(Id datasourceId);

    /**
     * Take the lock even if already held.
     * This changes the owner.
     * This resets the refresh index.
     *
     * Return new lock ownership/session Id or null if if the "oldlockSession" did not match (someone else has grabbed the lock).
     */
    public Id /*new ownership */ grabLock(Id datasourceId, Id oldlockSession);


    // FUTURE

//    /**
//     * Refresh a set of locks. This operation is blocking.
//     *
//     * Returns a set of {@link Id} for locks in the lock set that no longer exist, either
//     * because they have been released or have timed-out.
//     */
//    public Set<Id> refreshLocks(Set<Id> lockSet);

    /** Release the lock for a data source. This operation does not fail if there is no lock. */
    public void releaseLock(Id datasourceId, Id lockSession);
}
