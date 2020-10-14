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

package org.seaborne.delta.server.local.patchstores.zk;

import static org.seaborne.delta.zk.Zk.zkPath;

public class ZkConst {

    /* The ZooKeeper naming:
     * /delta/lock                          Store-wide lock
     * /delta/activeLocks/NAME              Indicate a log is active.
     *
     * Per log:
     * /delta/logs/NAME
     * /delta/logs/NAME/dsd
     * /delta/logs/NAME/lock
     * /delta/logs/NAME/state               (first_version: ,  DataSourceDescription)
     * /delta/logs/NAME/versions/00000000   Patch ids.
     * /delta/logs/NAME/header/00000000     Patches, JSON state (optional).
     *
     *
     * When including patch storage:
     *   /delta/logs/NAME/patches/
     */

    // Convention: p* is a path, n* is a zNode name.

    // Server
    static final String pRoot           = "/delta";
    static final String pLogs           = zkPath(pRoot, "logs");
    static final String pStoreLock      = zkPath(pRoot, "lock");
    static final String pActiveLogs     = zkPath(pRoot, "activeLogs");

    // Per patch log names.
    static final String nDsd            = "dsd";
    static final String nState          = "state";
    static final String nPatches        = "patches";
    static final String nLock           = "lock";
    static final String nLockState      = "noprefixlockState";

    // Version to id.
    static final String nVersions       = "versions";
    // Id to (version, id, prev).
    static final String nHeaders        = "header";
}
