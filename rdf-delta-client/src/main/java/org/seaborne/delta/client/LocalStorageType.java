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

package org.seaborne.delta.client;

import java.util.function.Supplier;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb2.DatabaseMgr;
import org.seaborne.delta.lib.DatasetGraphEmpty;

/**
 * Type of data persistence for a {@link Zone} managed dataset.
 * If a {@code DataSource} is "ephemeral" it disappears when the JVM ends.
 * It has no state across JVM instances.  
 */

public enum LocalStorageType {
    /** TDB storage */
    TDB(false, "TDB", TDBFactory::createDatasetGraph), 
    TDB2(false, "TDB2", DatabaseMgr::createDatasetGraph), 
    //FILE(false, "file"),
    /** In-memory and ephemeral */
    MEM(true, "mem", DatasetGraphFactory::createTxnMem),
    /** External storage (not zone managed). */
    EXTERNAL(true, "external", ()->null),
    /** No persistent tracking, just within this JVM */
    NONE(true, "none", DatasetGraphEmpty::new);
    ;
    private final boolean ephemeral ;
    private String typeName ;
    
    private LocalStorageType(boolean ephemeral, String typeName, Supplier<DatasetGraph> storage) {
        this.ephemeral = ephemeral;
        this.typeName = typeName;
    }
    
    public static LocalStorageType fromString(String string) {
        if ( string == null ) return null;
        if ( string.equalsIgnoreCase(TDB.typeName())) return TDB;
        if ( string.equalsIgnoreCase(MEM.typeName())) return MEM;
        if ( string.equalsIgnoreCase(EXTERNAL.typeName())) return EXTERNAL;
        if ( string.equalsIgnoreCase(NONE.typeName())) return NONE;
        return null;
        //throw new DeltaException("No storage type constant for '"+string+"'"); 
    }
    
    public boolean isEphemeral() { return ephemeral; }
    public String typeName() { return typeName; }
}

    