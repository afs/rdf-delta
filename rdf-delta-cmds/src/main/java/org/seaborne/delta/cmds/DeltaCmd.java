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
import org.apache.jena.atlas.web.HttpException ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.DeltaLinkHTTP ;
import org.seaborne.delta.link.DeltaLink ;

abstract public class DeltaCmd extends CmdGeneral {
   
    // Environment variable, for commands, for the remote server to work with.
    public static final String ENV_SERVER_URL =  "DELTA_SERVER_URL";

    static ArgDecl argServer            = new ArgDecl(true, "server");
    static ArgDecl argLogName           = new ArgDecl(true, "log", "dsrc", "dataset");
    static ArgDecl argDataSourceURI     = new ArgDecl(true, "uri", "dsrcuri");
    
    public DeltaCmd(String[] argv) {
        super(argv) ;
        super.add(argServer);
    }
    
    @Override
    final
    protected void processModulesAndArgs() {
        
        // Set a default for serverURL
        serverURL = System.getenv(ENV_SERVER_URL);
        
        super.processModulesAndArgs();
        
        if ( serverURL == null && ! contains(argServer) )
            throw new CmdException("Required: --server URL");
        checkForMandatoryArgs();
        
        if ( contains(argServer) ) {
            serverURL = getValue(argServer);
        }
        if ( contains(argLogName) ) {
            dataSourceName = getValue(argLogName);
            
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
            execCmd();
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
            descriptions = dLink.listDescriptions();
        return descriptions;
    }
    
    protected DataSourceDescription getDescription() {
        if ( description == null ) {
            description = 
                getDescriptions().stream()
                    .filter(dsd-> Objects.equals(dsd.getName(), dataSourceName) || Objects.equals(dsd.getUri(), dataSourceURI))
                    .findFirst().orElse(null);
            if ( description == null )
                throw new CmdException("Source '"+dataSourceName+"' does not exist");
        }
        return description;
    }
    
//    protected void ping() {
//        try {
//            dLink.ping();
//        } catch (HttpException ex) {
//            throw new CmdException(messageFromHttpException(ex));
//        } catch (JsonException ex) {
//            throw new CmdException("Not an RDF Patch server"); 
//        }
//    }
//    
//    // Library of operations on the DeltaLink.
//    
//    protected void list() {
//        List <DataSourceDescription> all = getDescriptions();
//        if ( all.isEmpty()) {
//            System.out.println("-- No logs --");
//            return ;
//        }
//        all.forEach(dsd->{
//            PatchLogInfo logInfo = dLink.getPatchLogInfo(dsd.id);
//            if ( logInfo != null ) {
//                System.out.print(
//                    String.format("[%s %s <%s> [%d,%d] %s]\n", dsd.id, dsd.name, dsd.uri, logInfo.minVersion, logInfo.maxVersion, 
//                                  (logInfo.latestPatch==null)?"--":logInfo.latestPatch.toString()));
//            }
//            else
//                System.out.println(dsd);
//            
//        });
//    }
//    
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
        return find(dsd-> Objects.equals(dsd.getName(), uri)) ;
    }
    
    protected Optional<Id> findByName(String name) {
        return find(dsd-> Objects.equals(dsd.getName(), name)) ;
    }

    protected Optional<Id> find(Predicate<DataSourceDescription> predicate) {
        List <DataSourceDescription> all = dLink.listDescriptions();
        return 
            all.stream()
               .filter(predicate)
               .findFirst()
               .map(dsd->dsd.getId());
    }
}
