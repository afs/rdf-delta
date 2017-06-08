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

package org.seaborne.delta.cmds;

import java.util.Optional ;

import jena.cmd.CmdException ;
import org.seaborne.delta.Id ;

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
        return getCommandName()+" --server URL [--uri=uri] NAME ....";
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
        // check
        super.getValues(argDataSourceURI).forEach(this::checkCmdName);
        super.getValues(argDataSourceURI).forEach(this::checkCmdURI);
        super.getPositional().forEach(this::checkCmdName);
        // execute
        super.getValues(argDataSourceURI).forEach(this::execCmdName);
        super.getValues(argDataSourceURI).forEach(this::execCmdURI);
        super.getPositional().forEach(this::execCmdName);
    }
    
    protected abstract void execCmdName(String name);
    protected abstract void execCmdURI(String uriStr);
    
    // default implementation - check exists. 
    
    protected void checkCmdName(String name) {
        Optional<Id> opt = findByName(name);
        if ( ! opt.isPresent() )
            throw new CmdException("Source '"+name+"' does not exist"); 
    }
    
    protected void checkCmdURI(String uriStr) {
        Optional<Id> opt = findByURI(uriStr);
        if ( ! opt.isPresent() )
            throw new CmdException("Source <"+uriStr+"> does not exist"); 
    }
}
