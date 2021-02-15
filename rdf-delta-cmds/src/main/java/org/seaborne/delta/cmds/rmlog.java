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

import java.util.Optional ;

import org.apache.jena.cmd.CmdException ;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id ;

/** Remove a log */
public class rmlog extends DeltaCmdServerOp {

    public static void main(String... args) {
        new rmlog(args).mainRun();
    }

    public rmlog(String[] argv) {
        super(argv) ;
    }

    @Override
    protected void execCmdName(String name) {
        Optional<DataSourceDescription> opt = findByName(name);
        if ( ! opt.isPresent() )
            throw new CmdException("Source '"+name+"' does not exist");
        DataSourceDescription dsd = opt.get();
        execRm(dsd);
    }

    private void execRm(DataSourceDescription dsd) { Id dsRef = dsd.getId();
    dLink.removeDataSource(dsRef);
    System.out.println("Deleted "+dsd);}

    @Override
    protected void execCmdURI(String uriStr) {
        Optional<DataSourceDescription> opt = findByURI(uriStr);
        if ( ! opt.isPresent() )
            throw new CmdException("Source <"+uriStr+"> does not exist");
        DataSourceDescription dsd = opt.get();
        execRm(dsd);
    }
}
