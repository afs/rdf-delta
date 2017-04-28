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

import jena.cmd.ArgDecl ;
import jena.cmd.CmdException ;
import jena.cmd.CmdGeneral ;
import org.apache.commons.lang3.StringUtils ;
import org.apache.jena.atlas.json.JsonException ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.atlas.web.HttpException ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.link.DeltaLink ;

abstract public class DeltaCmd extends CmdGeneral {

    static { LogCtl.setCmdLogging() ; }
    
    static ArgDecl argServer            = new ArgDecl(true, "server");
    static ArgDecl argDataSourceName    = new ArgDecl(true, "dsrc", "log", "dataset");
    static ArgDecl argDataSourceURL     = new ArgDecl(true, "dsrcurl");
    
    public DeltaCmd(String[] argv) {
        super(argv) ;
        super.add(argServer);

        // Added as necessary by specific commands.
        //super.add(argDataSourceName);
        //super.add(argDataSourceURL);
    }
    
    @Override
    final
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();
        
        if ( ! contains(argServer) )
            throw new CmdException("Required: --server URL");
        checkForMandatoryArgs();
        
        if ( contains(argServer) ) {
            serverURL = getValue(argServer);
        }
        if ( contains(argDataSourceName) ) {
            dataSourceName = getValue(argDataSourceName);
            
            if ( dataSourceName.isEmpty() )
                throw new CmdException("Empty string for data source name");
            
            if ( StringUtils.containsAny(dataSourceURL, "/ ?#") )
                throw new CmdException("Illegal character in data source name");
            
            if ( ! dataSourceName.matches("^[\\w-_]+$") )
            {}
            
            String s = serverURL;
            if ( ! s.endsWith("/") )
                s = s+"/";
            dataSourceURL = s + dataSourceName;
        }
        if ( contains(argDataSourceURL) ) {
            dataSourceURL = getValue(argDataSourceURL);
        }
        
        dLink = DeltaLinkHTTP.connect(serverURL);
    }
    
    protected abstract void checkForMandatoryArgs();

    protected String    serverURL       = null ;
    protected String    dataSourceName  = null ;
    protected String    dataSourceURL   = null ;
    protected DeltaLink dLink           = null ;

    @Override
    protected void exec() {
        Id clientId = Id.create() ;
        try { 
            dLink.register(clientId) ;
            try { execCmd() ; }
            finally { 
                if ( dLink.isRegistered() )
                    dLink.deregister();
            }
        }
        catch (HttpException ex) { throw new CmdException(messageFromHttpException(ex)) ; }
    }

    // --- The commands 
    @Override
    protected String getCommandName() {
        String name = this.getClass().getSimpleName();
        switch (name) {
            case "": name = "anon"; break;
            case "[]": name = "anon[]"; break;
        }
        return name;
    }
    
    protected abstract void execCmd();

    protected void ping() {
        try {
            dLink.ping();
        } catch (HttpException ex) {
            throw new CmdException(messageFromHttpException(ex));
        } catch (JsonException ex) {
            throw new CmdException("Not an RDF Patch server"); 
        }
    }

    protected String messageFromHttpException(HttpException ex) {
        Throwable ex2 = ex;
        if ( ex.getCause() != null )
            ex2 = ex.getCause();
        return ex2.getMessage();
    }
    
    // Library of operations on the DeltaLink.
    
    protected void list() {
        List <DataSourceDescription> all = dLink.allDescriptions();
        if ( all.isEmpty())
            System.out.println("-- No logs --");
        else    
            all.forEach(System.out::println);
    }

    protected void create(String name, String url) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(url);
        
        List <DataSourceDescription> all = dLink.allDescriptions();
        boolean b = all.stream().anyMatch(dsd-> Objects.equals(dsd.name, name));
        if ( b )
            throw new CmdException("Source '"+name+"' already exists");
        Id id = dLink.newDataSource(name, url);
        DataSourceDescription dsd = dLink.getDataSourceDescription(id);
        System.out.println("Created "+dsd);
    }

    protected void hide(String name) {
        Optional<Id> opt = find(name);
        if ( ! opt.isPresent() )
            throw new CmdException("Source '"+name+"' does not exist");
        Id dsRef = opt.get(); 
        dLink.removeDataset(dsRef);
    }

    // Find DSD for the command line data source.
    protected Optional<Id> find() {
        return null;
    }
    
    
    protected Optional<Id> find(String name) {
        Objects.requireNonNull(name);
        
        List <DataSourceDescription> all = dLink.allDescriptions();
        return 
            all.stream()
               .filter(dsd-> Objects.equals(dsd.name, name))
               .findFirst()
               .map(dsd->dsd.id);
    }

}
