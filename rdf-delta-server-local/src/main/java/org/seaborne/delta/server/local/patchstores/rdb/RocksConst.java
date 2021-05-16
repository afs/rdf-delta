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

package org.seaborne.delta.server.local.patchstores.rdb;

import static org.apache.jena.atlas.lib.StrUtils.asUTF8bytes;

public class RocksConst {

    public static final String databaseFilename = "rdb";

    public static final String CF_VERSION_ID   = "versionToId";
    public static final String CF_ID_ENTRY     = "idToLogEntry";
    public static final String CF_PATCH        = "patchStorage";

    public static final byte[] B_CF_VERSION_ID = asUTF8bytes(CF_VERSION_ID);
    public static final byte[] B_CF_ID_ENTRY   = asUTF8bytes(CF_ID_ENTRY);
    public static final byte[] B_CF_PATCH      = asUTF8bytes(CF_PATCH);

}
