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

package org.seaborne.delta.cmds;

import java.util.Objects;

import jena.cmd.CmdException ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.Id ;

/** Move (rename) a log.
 * This changes the short name; the URI remains the same.
 */

public class cplog extends DeltaCmd_2 {

    public static void main(String... args) {
        new cplog(args).mainRun();
    }

    public cplog(String[] argv) {
        super(argv) ;
    }

    @Override
    protected void execCmd(String oldName, String newName) {
        copy(oldName, newName);
    }

    protected void copy(String oldName, String newName) {
        // Checking.
        Objects.requireNonNull(oldName, "Old name");
        Objects.requireNonNull(newName, "New name");
        DataSourceDescription srcDsd = dLink.getDataSourceDescriptionByName(oldName);
        if ( srcDsd == null )
            throw new CmdException("Patch log '"+oldName+"' does not exist");
        DataSourceDescription dstDsd = dLink.getDataSourceDescriptionByName(newName);
        if ( dstDsd != null )
            throw new CmdException("Patch log '"+newName+"' already exists");

        Id id = srcDsd.getId();
        try {
            dLink.copyDataSource(id, oldName, newName);
        } catch (DeltaException ex) {
            throw new CmdException("Failed to rename log '"+oldName+"' to '"+newName+"' : "+ex.getMessage());
        }
    }
}
