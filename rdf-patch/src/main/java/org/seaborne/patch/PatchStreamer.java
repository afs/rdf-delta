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

package org.seaborne.patch;

import static org.apache.jena.riot.tokens.TokenType.EOF ;

import java.util.NoSuchElementException ;

import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.Tokenizer ;


// XXX Unused : 
// This is a pure stream based tokens to RDFChangesApply processor
// PatchReader gathers up a whole line then calculates the operation.  
public class PatchStreamer
{
    private boolean             finished = false ;
    private final Tokenizer     tokens ;

    public PatchStreamer(String label, Tokenizer tokens) {
        this.tokens = tokens ;
    }

    public boolean next() {
        if ( finished )
            return false ;

        if ( !tokens.hasNext() ) {
            finished = true ;
            return false ;
        }
        finished = processOneLine() ;
        return finished ;
    }

    // Process. Return "finished"
    private boolean processOneLine() {
//        boolean isDirective = false ;
//        Token token = tokens.next() ;
//
//        // TIO2 row.
//
//        if ( token.hasType(EOF) )
//            return true ;
//
//        if ( ! token.hasType(KEYWORD) )
//            throw new TIOException("TIO Tuple has an empty change code") ;
//        String str = token.getImage() ;
//        if ( str.isEmpty() )
//            throw new TIOException("Not a change code: '"+str+"'") ;
//
//        ChangeCode code ; 
//        try { code = ChangeCode.valueOf(str) ; }
//        catch (IllegalArgumentException ex) { throw new TIOException("Not a change code: '"+str+"'") ; }
//        switch (code) {
//            case ADD_QUAD :
//            {
//                Token t = read() ;
//                Node g = null ;
//                if ( t != null )
//                    g = t.asNode() ;
//                    
//                read().asNode() ;
//                Node s = read().asNode() ;
//                Node p = read().asNode() ;
//                Node o = read().asNode() ;
//                // read 4.
//                break ;
//            }
//            case DEL_QUAD :
//                break ;
//            case ADD_PREFIX :
//                // Read 2
//                break ;
//            case DEL_PREFIX :
//                break ;
//            case SET_BASE :
//                break ;
//            case TXN_BEGIN :
//                // TB id= parent=
//                break ;
//            case TXN_ABORT :
//                break ;
//            case TXN_COMMIT :
//                break ;
//            case TXN_PROMOTE :
//                break ;
//            default :
//                break ;
//
//        }
        // Peek?
        return false ;
    }
    
    private Token read() {
        Token token = tokens.next() ;
        if ( token.hasType(EOF) )
            throw new NoSuchElementException() ;
        return token ;
    }
}
