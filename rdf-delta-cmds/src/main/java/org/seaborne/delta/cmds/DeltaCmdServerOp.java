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

import java.util.List;
import java.util.Optional ;

import jena.cmd.CmdException ;
import org.seaborne.delta.DataSourceDescription;

/** Base for operations of the form:
 *   cmd --server= ds1 ds2 ds3
 *   cmd --server= --uri uri1 --uri uri2
 */
public abstract class DeltaCmdServerOp extends DeltaCmd {

    public DeltaCmdServerOp(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server=URL NAME ....";
    }

//    @Override
//    protected void execCmd() {
//        hide(super.dataSourceName, super.dataSourceURI);
//    }

    @Override
    protected void checkForMandatoryArgs() {
        if ( !contains(argLogName) && !contains(argDataSourceURI) && getPositional().isEmpty() )
            throw new CmdException("Nothing to act on: "+getSummary());
    }

    @Override
    protected void execCmd() {
        List<String> names = super.getValues(argLogName);
        List<String> uris = super.getValues(argDataSourceURI);
        List<String> positionals = super.getPositional();

        if ( names.size() + uris.size() + positionals.size() > 1 )
            throw new CmdException("Multiple logs specificed: use one of NAME or '--log=NAME' or '--uri=URI'");

        // Only one of these will be non-empty, and it will have one item.
        names.forEach(this::checkCmdName);
        uris.forEach(this::checkCmdURI);
        positionals.forEach(this::checkCmdName);
        // execute
        names.forEach(this::execCmdName);
        uris.forEach(this::execCmdURI);
        positionals.forEach(this::execCmdName);
    }


    protected abstract void execCmdName(String name);
    protected abstract void execCmdURI(String uriStr);

    // default implementation - check exists.

    protected void checkCmdName(String name) {
        Optional<DataSourceDescription> opt = findByName(name);
        if ( ! opt.isPresent() )
            throw new CmdException("Source '"+name+"' does not exist");
    }

    protected void checkCmdURI(String uriStr) {
        Optional<DataSourceDescription> opt = findByURI(uriStr);
        if ( ! opt.isPresent() )
            throw new CmdException("Source <"+uriStr+"> does not exist");
    }
}
