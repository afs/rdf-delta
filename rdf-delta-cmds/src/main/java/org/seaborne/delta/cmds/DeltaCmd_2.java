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

/** Base for operations referring to two logs.
 *  dcmd op2 --server SERVER LOG1 LOG2
 */
public abstract class DeltaCmd_2 extends DeltaCmd {

    public DeltaCmd_2(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server=URL LOG1 LOG2";
    }

    @Override
    protected void checkForMandatoryArgs() {
        if ( !contains(argLogName) && !contains(argDataSourceURI) && getPositional().isEmpty() )
            throw new CmdException("Nothing to act on: "+getSummary());
    }

    @Override
    protected void execCmd() {
        List<String> positionals = super.getPositional();
        if ( positionals.size() != 2 )
            throw new CmdException("Two patch log names argument required: "+this.getCommandName()+ " NAME1 NAME2");
        String srcLog = positionals.get(0);
        String dstLog = positionals.get(1);

        boolean existsSrc = logExists(srcLog);
        if ( !existsSrc )
            throw new CmdException("Source '"+srcLog+"' does not exist");
        boolean existsDst = logExists(dstLog);
        if ( existsDst )
            throw new CmdException("Source '"+srcLog+"' already exists");
        execCmd(srcLog, dstLog);
    }

    protected abstract void execCmd(String name1, String name2);

    protected boolean logExists(String name) {
        Optional<DataSourceDescription> opt = findByName(name);
        return opt.isPresent();
    }
}
