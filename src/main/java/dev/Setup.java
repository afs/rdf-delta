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

package dev;

import java.util.ArrayList ;
import java.util.List ;

import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.server.DPS ;
import org.seaborne.delta.server.PatchHandler ;
import org.seaborne.delta.server.handlers.* ;

/** Configuration for the patch receiver */ 
public class Setup {
    public static PatchHandler[] handlers(DatasetGraph dsg) { 
        List<PatchHandler> x = new ArrayList<>() ;
        if ( dsg != null )
            x.add(new PHandlerLocalDB(dsg)) ;
        x.add(new PHandlerOutput(System.out)) ;
        x.add(new PHandlerGSPOutput()) ;
        x.add(new PHandlerGSP().addEndpoint("http://localhost:3030/ds/update")) ;
        x.add(new PHandlerToFile()) ;
        x.add(new PHandlerLog(DPS.LOG)) ;
        
        return x.toArray(new PatchHandler[0]) ;
    }
}
