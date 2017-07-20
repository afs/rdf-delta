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

package org.seaborne.delta.server.http;

import static org.seaborne.delta.server.http.DeltaAction.errorBadRequest ;

import java.util.Locale ;
import java.util.Map;
import java.util.UUID ;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.riot.web.HttpNames ;
import org.seaborne.delta.DeltaBadRequestException ;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.RegToken;

/** Parsed arguments for Patch and Fetch. 
 * <p>
 * The query string is used for arguments.
 *  <ul>
 *  <li><tt>dataset</tt> &ndash; Id or URI for the datasource 
 *  <li><tt>patch</tt> &ndash; patch id (for fetch) 
 *  <li><tt>version</tt> &ndash; version number
 *  <li><tt>ref</tt> &ndash; pointer to predefined arguments [Not Implemented]
 *  <li><tt>zone</tt> &ndash; pointer to predefined arguments [Not Implemented] 
 *  </ul>
 */
public class Args {
    
    private static Map<String, Args> registration = new ConcurrentHashMap<>(); 

    public static Args argsParams(HttpServletRequest request) {
        String ref = request.getParameter(DeltaConst.paramRef);
        if ( ref != null )
            return registration.get(ref);
        String datasourceName = request.getParameter(DeltaConst.paramDatasource);
        String patchIdStr = request.getParameter(DeltaConst.paramPatch);
        String versionStr = request.getParameter(DeltaConst.paramVersion);
        String clientIdStr = request.getParameter(DeltaConst.paramClient);
        String regTokenStr = request.getParameter(DeltaConst.paramReg);  

        if ( datasourceName != null ) {
            if ( ! DeltaOps.isValidName(datasourceName) )
                throw new DeltaBadRequestException("Bad name for a data source: "+datasourceName);
        }
        
        Id patchId = patchIdStr == null ? null : Id.fromString(patchIdStr);
        Id clientId = clientIdStr == null ? null : Id.fromString(clientIdStr);
        RegToken regToken = regTokenStr == null ? null : new RegToken(regTokenStr);
        Long version = versionStr == null ?null : new Long(versionStr);
        return new Args(request, datasourceName, patchId, version, clientId, regToken);
    }
    
    /** Process an HTTP request to extract the arguments.
     * Two styles are supported: the preferred RESTful coatainer style:
     *      * <ul>
     * <li>Append patch: {@code POST} to <tt>/{name}/</tt>
     * <li>Get patch: {@code GET} from <tt>/{name}/log/{id or version}</tt>
     * </ul>
     * but also the same information using query string parameters:
     * <ul>
     * <li>Append patch: {@code POST} to <tt>/{srvName}?datasource={name}</tt>
     * <li>Get patch:    {@code GET} from <tt>/{srvName}?datasource={name}&id={id}</tt> or <tt>/{srcName}?datasource={name}&version={version}</tt>.  
     * </ul>
     * where <tt>{srvName}</tt> is the service name.
     * <p>
     * The registration token is always a query string parameter.  
     */
    public static Args pathArgs(HttpServletRequest request) {
        // Process, arguments for any operation.
        // Firstly, do as query string parameters to set defauls.
        String datasourceName = request.getParameter(DeltaConst.paramDatasource);
        String patchIdStr = request.getParameter(DeltaConst.paramPatch);
        String versionStr = request.getParameter(DeltaConst.paramVersion);
        
        // Should be null.
        String clientIdStr = request.getParameter(DeltaConst.paramClient);
        // Will happen for an append.
        String regTokenStr = request.getParameter(DeltaConst.paramReg);  

        Id patchId = patchIdStr == null ? null : Id.fromString(patchIdStr);
        Id clientId = clientIdStr == null ? null : Id.fromString(clientIdStr);
        RegToken regToken = regTokenStr == null ? null : new RegToken(regTokenStr);
        
        Long version = null ;
        if ( versionStr != null ) {
            try { version = Long.parseLong(versionStr); }
            catch (NumberFormatException ex) { errorBadRequest("Can't parse version: "+versionStr) ; }  
        }
        
        /* Now the preferred URI: 
         *     /servlet/{name}/
         *     /servlet/{name}/patch/{version}
         *     /servlet/{name}/patch/{id}
         */
        
        String uri = request.getRequestURI();
        String x = getTrailing(request);
        if ( x.isEmpty() ) {
            // No name.
            return new Args(request, datasourceName, patchId, version, clientId, regToken);
        }
        
        if ( ! x.startsWith("/") )
            errorBadRequest("Bad URI: "+uri); 
            
//        int idxStart = 1;
//        int idxFinish = x.indexOf('/', idxStart);
//        if ( idxFinish > 0 )
//            datasourceName = x.substring(idxStart, idxFinish);
//        else
//            datasourceName = x.substring(idxStart);

        // z[0] is always empty because x starts with "/"
        String[] z = x.split("/");
        String msg="";
        
        // Cant happen?
        if ( z.length == 0 ) throw new IllegalArgumentException("zero length URI split array") ; 
        if ( z.length == 1 ) errorBadRequest("No name given");
        if ( z.length > 3 )
            errorBadRequest("URI path has too many components.");
        // Case length 2 or 3.
        datasourceName = z[1];
        if ( z.length == 3 ) {
            String patchStr = z[2];
            
            if ( patchStr.isEmpty() )
                errorBadRequest("Patch ref empty");
            if ( Id.maybeUUID(patchStr) ) {
                patchId = Id.parseId(patchStr, null);
                if ( patchId == null )
                    errorBadRequest("Can't parse id: "+versionStr);
            } else {
                version = parseVersion(patchStr, null);
            }
        }
        return new Args(request, datasourceName, patchId, version, clientId, regToken);
    }
    
    private static UUID parseUUID(String patchStr, UUID dft) {
        try { 
            return UUID.fromString(patchStr);
        } catch (IllegalArgumentException ex) {
            return dft;
        }
    }

    private static Long parseVersion(String intStr, Long dft) {
        if ( intStr.isEmpty() )
            return dft;
        // Numbers only.
        if ( intStr.startsWith("+") || intStr.startsWith("-") )
            return dft;
        try {  
            return new Long(intStr);
        } catch (NumberFormatException ex) {
            return dft;
        }
    }
    
    /**
     * Get the trailing part of a request URI.
     * The URI is assumed to be in the form "/context/servlet/trailing". 
     * @return The trailing part or   
     */
    private static String getTrailing(HttpServletRequest request) {
//        Log.info(this, "URI                     = '"+request.getRequestURI()) ;
//        Log.info(this, "Context path            = '"+request.getContextPath()+"'") ;
//        Log.info(this, "Servlet path            = '"+request.getServletPath()+"'") ;
//        // Only valid for webapps.
//        ServletContext cxt = this.getServletContext() ;
//        Log.info(this, "ServletContext path     = '"+cxt.getContextPath()+"'") ;

        // URL naming version ; URI is "context/servletname/   
        String servletPath = request.getServletPath() ;
        String uri = request.getRequestURI() ;
        String x = uri ;
        if ( uri.equals(servletPath) )
            // Dispatch by servlet filter 
            return x;
        if ( uri.startsWith(servletPath) )
            x = uri.substring(servletPath.length()) ;
        return x ;
    } 

    public final String url;
    public final String method;
    public final String datasourceName;
    public final Id patchId;
    public final Long version;
    public final Id clientId;
    public final RegToken regToken;

    public Args(HttpServletRequest request, String datasourceName, Id patchId, Long verStr, Id clientId, RegToken regToken) {
        super();
        this.url = ServerLib.url(request);
        this.method = request.getMethod().toUpperCase(Locale.ROOT);
        switch(this.method) {
            case HttpNames.METHOD_GET:
            case HttpNames.METHOD_POST:
            case HttpNames.METHOD_PATCH:
                break;
            default:
                throw new IllegalArgumentException("Wrong method: "+method);
        }
        
        this.datasourceName = datasourceName;
        this.patchId = patchId;
        this.version = verStr;
        this.clientId = clientId;
        this.regToken = regToken;
    }
}
