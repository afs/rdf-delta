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

package org.seaborne.delta;

import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.link.DeltaNotRegisteredException ;

/** Exception to throw when a request is wrong in some way */ 
public class DeltaHttpException extends DeltaException {
    private final int statusCode ;  
    
    public DeltaHttpException(int code, String msg) {
        super(msg) ;
        statusCode = code ;
    }

    public int getStatusCode() {
        return statusCode;
    }
    
    /** Convert to original exception. */
    public static DeltaHttpException extract(DeltaHttpException ex) {
        int statusCode = ex.getStatusCode();
        String msg = ex.getMessage(); 
        switch(statusCode) {
            case HttpSC.BAD_REQUEST_400:    return new DeltaBadRequestException(msg); 
            case HttpSC.NOT_FOUND_404:      return new DeltaNotFoundException(msg); 
            case HttpSC.UNAUTHORIZED_401:   return new DeltaNotRegisteredException(msg); 
            case HttpSC.FORBIDDEN_403:       
            default:
                return ex;
        }
    }
    
    @Override
    public String toString() {
        return getStatusCode()+" : "+getMessage(); 
    }
    
//    @Override
//    public Throwable fillInStackTrace() {
//        return this ;
//    }
}
