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

import java.util.List ;
import java.util.Objects ;
import java.util.Optional ;

import jena.cmd.CmdException ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.Id ;

/** Create a new log */
public class mklog extends DeltaCmdServerOp {
    
    public static void main(String... args) {
        new mklog(args).mainRun();
    }

    public mklog(String[] argv) {
        super(argv) ;
    }

    @Override
    protected void execCmdName(String name) {
        String uri = "http://base/"+name;
        create(name, uri);
    }

    @Override
    protected void execCmdURI(String uriStr) {
        int idx = uriStr.lastIndexOf('#');
        if ( idx < 0 )
            idx = uriStr.lastIndexOf('/');
        if ( idx < 0 || idx == uriStr.length() )
            throw new CmdException("Can't determine a datasource name from <"+uriStr+">");
        String name = uriStr.substring(idx+1);
        if ( name.isEmpty() )
            throw new CmdException("Can't determine a datasource name from <"+uriStr+">");
        create(name, uriStr);
    }

    protected void create(String name, String url) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(url);
        
        List <DataSourceDescription> all = dLink.listDescriptions();
        boolean b = all.stream().anyMatch(dsd-> Objects.equals(dsd.getName(), name));
        if ( b )
            throw new CmdException("Source '"+name+"' already exists");
        Id id = dLink.newDataSource(name, url);
        DataSourceDescription dsd = dLink.getDataSourceDescription(id);
        System.out.println("Created "+dsd);
    }
    
    @Override
    protected void checkCmdName(String name) {
        Optional<Id> opt = findByName(name);
        if ( opt.isPresent() )
            throw new CmdException("Source '"+name+"' already exists"); 
    }
    
    @Override
    protected void checkCmdURI(String uriStr) {
        Optional<Id> opt = findByURI(uriStr);
        if ( opt.isPresent() )
            throw new CmdException("Source <"+uriStr+"> already exists"); 
    }
}
