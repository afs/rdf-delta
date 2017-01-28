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

import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest ;

import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger ;

/** Fetch a patch from a container: id is part of the path name.  */
public class S_Fetch extends FetchBase {

    static public Logger LOG = Delta.getDeltaLogger("Fetch");
    
    public S_Fetch(AtomicReference<DeltaLink> engine) {
        super(engine) ;
    }

    @Override
    protected Args getArgs(HttpServletRequest req) {
        throw new DeltaBadRequestException(HttpSC.INTERNAL_SERVER_ERROR_500, "Not implemented");
//        String x = req.getRequestURI();
//        int j = x.lastIndexOf('/');
//        if ( j < 0 )
//            throw new DeltaExceptionBadRequest("Failed to find the patch id");
//        return x.substring(j + 1);
        
    }

    @Override
    protected String getOpName() {
        return "fetch:path";
    }
}
