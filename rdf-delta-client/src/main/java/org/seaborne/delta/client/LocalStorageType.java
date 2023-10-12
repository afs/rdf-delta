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

package org.seaborne.delta.client;

/**
 * Type of data persistence for a {@link Zone} managed dataset.
 * If a {@code DataSource} is "ephemeral" it disappears when the JVM ends.
 * It has no state across JVM instances.
 */

public enum LocalStorageType {

    /** TDB storage */
    TDB(false, true, "TDB"),
    TDB2(false, true, "TDB2"),
    //FILE(false, true, "file"),

    /** In-memory and ephemeral */
    MEM(true, true, "mem"),

    /** External storage (not zone managed) with persistent tracked state. */
    EXTERNAL(false, false, "external"),

    /** No persistence tracking, just within this JVM and no persistent state. */
    NONE(true, false, "none");
    ;

    private String typeName ;
    private final boolean ephemeral ;
    private final boolean managedStorage ;

    private LocalStorageType(boolean ephemeral, boolean managedStorage, String typeName) {
        this.ephemeral = ephemeral;
        this.managedStorage = managedStorage;
        this.typeName = typeName;
    }

    public static LocalStorageType fromString(String string) {
        if ( string == null ) return null;
        if ( string.equalsIgnoreCase(TDB.typeName())) return TDB;
        if ( string.equalsIgnoreCase(TDB2.typeName())) return TDB2;
        if ( string.equalsIgnoreCase(MEM.typeName())) return MEM;
        if ( string.equalsIgnoreCase(EXTERNAL.typeName())) return EXTERNAL;
        if ( string.equalsIgnoreCase(NONE.typeName())) return NONE;
        return null;
        //throw new DeltaException("No storage type constant for '"+string+"'");
    }

    /** If "ephemeral", the tracked state disappears when the JVM ends. */
    public boolean isEphemeral() { return ephemeral; }

    /** If "managedStorage", the local Zone is managing storage. */
    public boolean managedStorage() { return managedStorage; }

    /** Name for presentation purposes. */
    // toString() left to default enum behaviour.
    public String typeName() { return typeName; }
}

