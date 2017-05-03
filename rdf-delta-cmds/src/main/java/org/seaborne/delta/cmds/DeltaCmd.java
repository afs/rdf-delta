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
import java.util.function.Predicate ;

import jena.cmd.ArgDecl ;
import jena.cmd.CmdException ;
import jena.cmd.CmdGeneral ;
import org.apache.commons.lang3.StringUtils ;
import org.apache.jena.atlas.json.JsonException ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.atlas.web.HttpException ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.link.DeltaLink ;

abstract public class DeltaCmd extends CmdGeneral {

    static { LogCtl.setCmdLogging() ; }
    
    static ArgDecl argServer            = new ArgDecl(true, "server");
    static ArgDecl argDataSourceName    = new ArgDecl(true, "dsrc", "log", "dataset");
    static ArgDecl argDataSourceURI     = new ArgDecl(true, "dsrcurl", "uri", "url");
    
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
            
            if ( StringUtils.containsAny(dataSourceName, "/ ?#") ) {
                // First bad character:
                int idx = StringUtils.indexOfAny(serverURL, dataSourceName);
                char ch = dataSourceName.charAt(idx);
                throw new CmdException(String.format("Illegal character '%c' in data source name: '%s'", ch, dataSourceName));
            }
            if ( ! DeltaOps.isValidName(dataSourceName) )
                throw new CmdException("Not a valid data source name: '"+dataSourceName+"'");
            
            String s = serverURL;
            if ( ! s.endsWith("/") )
                s = s+"/";
            dataSourceURI = s + dataSourceName;
        }
        if ( contains(argDataSourceURI) ) {
            dataSourceURI = getValue(argDataSourceURI);
            if ( StringUtils.containsAny(dataSourceURI, "<>?#") )
                throw new CmdException("Bad data source URI: '"+dataSourceURI+"'");
        }
        dLink = DeltaLinkHTTP.connect(serverURL);
    }
    
    protected abstract void checkForMandatoryArgs();

    protected String    serverURL           = null ;
    protected String    dataSourceName      = null ;
    protected String    dataSourceURI       = null ;
    protected DeltaLink dLink               = null ;
    protected Id clientId                   = Id.create() ;
    protected List<DataSourceDescription> descriptions = null ; 
    protected DataSourceDescription       description = null ;
    
    @Override
    protected void exec() {
        try { 
            dLink.register(clientId) ;
            try { execCmd() ; }
            finally {
                //if ( dLink.isRegistered() )
                    dLink.deregister();
            }
        }
        catch (HttpException ex) { throw new CmdException(messageFromHttpException(ex)) ; }
    }

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

    protected List<DataSourceDescription> getDescriptions() {
        if ( descriptions == null )
            descriptions = dLink.allDescriptions();
        return descriptions;
    }
    
    protected DataSourceDescription getDescription() {
        if ( description == null ) {
            description = 
                getDescriptions().stream()
                    .filter(dsd-> Objects.equals(dsd.name, dataSourceName) || Objects.equals(dsd.uri, dataSourceURI))
                    .findFirst().orElse(null);
            if ( description == null )
                throw new CmdException("Source '"+dataSourceName+"' does not exist");
        }
        return description;
    }
    
    protected Id getDataSourceRef() {
        return getDescription().getId();
    }
    
    // --- The commands 

    protected void ping() {
        try {
            dLink.ping();
        } catch (HttpException ex) {
            throw new CmdException(messageFromHttpException(ex));
        } catch (JsonException ex) {
            throw new CmdException("Not an RDF Patch server"); 
        }
    }
    
    // Library of operations on the DeltaLink.
    
    protected void list() {
        List <DataSourceDescription> all = getDescriptions();
        if ( all.isEmpty()) {
            System.out.println("-- No logs --");
            return ;
        }
        
        all.forEach(dsd->{
            System.out.print(dsd);
            PatchLogInfo logInfo = dLink.getPatchLogInfo(dsd.id);
            if ( logInfo != null )
                String.format("[%s, %s, <%s> [%d,%d] %s]", dsd.id, dsd.name, dsd.uri, logInfo.minVersion, logInfo.maxVersion, 
                              (logInfo.latestPatch==null)?"--":logInfo.latestPatch.toString());
            else
                System.out.println(dsd);
            
        });
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

    protected void hide(String name, String url) {
        Optional<Id> opt = find(name, url);
        if ( ! opt.isPresent() ) {
            String s = (name != null) ? name : url;  
            throw new CmdException("Source '"+s+"' does not exist");
        }
        Id dsRef = opt.get(); 
        dLink.removeDataset(dsRef);
    }

    protected String messageFromHttpException(HttpException ex) {
        Throwable ex2 = ex;
        if ( ex.getCause() != null )
            ex2 = ex.getCause();
        return ex2.getMessage();
    }

    protected Optional<Id> find(String name, String url) {
        if ( name != null )
            return findByName(name);
        if ( url != null )
            return findByName(url);
        throw new CmdException("No name or URI for the source");
    }
    
    protected Optional<Id> findByURI(String uri) {
        return find(dsd-> Objects.equals(dsd.name, uri)) ;
    }
    
    protected Optional<Id> findByName(String name) {
        return find(dsd-> Objects.equals(dsd.name, name)) ;
    }

    protected Optional<Id> find(Predicate<DataSourceDescription> predicate) {
        List <DataSourceDescription> all = dLink.allDescriptions();
        return 
            all.stream()
               .filter(predicate)
               .findFirst()
               .map(dsd->dsd.id);
    }
}
