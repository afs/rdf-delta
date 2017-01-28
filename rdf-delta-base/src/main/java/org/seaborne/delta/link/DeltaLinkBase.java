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

package org.seaborne.delta.link;

import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.Id;

/** Support for management of registrations for a {@link DeltaLink}.
 *  This implementation provides a simple in-memory link manager. 
 */
public abstract class DeltaLinkBase implements DeltaLink {
    
    protected final DeltaLinkMgr linkMgr = new DeltaLinkMgr();
    protected RegToken regToken = null;
    protected Id clientId = null; 
    
    @Override
    final
    public RegToken register(Id clientId) {
        if ( isRegistered() ) {
            if ( this.clientId.equals(clientId) ) {
                Log.warn(this,  "Already registered: "+clientId);
                return regToken; 
            } else {
                Log.error(this,  "Already registered under a different clientId: "+clientId);
                throw new DeltaBadRequestException("Already registered under a different clientId");
            }
        }
            
        this.clientId = clientId;
        regToken = linkMgr.register(clientId);
        return regToken;
    }

    @Override
    final
    public void deregister() {
        if ( regToken != null )
            linkMgr.deregister(regToken);
        regToken = null;
        // Retain the clientId.
    }

    /** Check whether a client id is registered for this link. */
    @Override
    final
    public boolean isRegistered() {
        return linkMgr.isRegistered(clientId); 
    }
    
    @Override
    public RegToken getRegToken() {
        return regToken;
    }

    @Override
    public Id getClientId() {
        return clientId;
    }
}
