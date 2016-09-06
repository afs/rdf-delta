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

package org.seaborne.delta.base;

import java.io.InputStream ;
import java.util.List ;

import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.riot.system.RiotLib ;
import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.Tokenizer ;
import org.apache.jena.riot.tokens.TokenizerFactory ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.tio.CommsException ;
import org.seaborne.patch.tio.TokenInputStream ;
import org.seaborne.patch.tio.TokenInputStreamBase ;

// Needs reworking: for efficiency, for less features
/** Must close the input stream */
public class PatchReader {
    private final TokenInputStream input ;
    
    public PatchReader(InputStream in) {
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerUTF8(in) ; 
        input = new TokenInputStreamBase(null, tokenizer) ;
    }

    /** Execute transactions until the input ends or something goes wrong. */
    public void apply(RDFChanges sink) {
        while(input.hasNext()) { 
            boolean b = apply1(sink);
            if ( !b )
                return ;
        }
    }
    
    public boolean hasMore() {
        return input.hasNext() ;
    }
    
    /** Execute one transaction.
     *  Return true if there is the possiblity of more.
     */
    public boolean apply1(RDFChanges sink) {
        // Abort if no end of transaction
        
        boolean oneTransaction = true ;  
        
        int lineNumber = 0 ;
        while(input.hasNext()) {
            List<Token> line = input.next() ;
            //System.err.println("Line = "+line);
            if ( line.isEmpty() )
                throw new CommsException("["+lineNumber+"] empty line") ;
            lineNumber ++ ;
            try { 
                boolean b = doOneLine(line, sink) ;
                if ( oneTransaction && b )
                    return true ;
            } catch (Exception ex) {
                ex.printStackTrace(System.err) ;
                sink.txnAbort();
                return false ;
            }
        }
        return false ;
    }
        
    private boolean doOneLine(List<Token> line, RDFChanges sink) {
        Token token1 = line.get(0) ;
        if ( ! token1.isWord() )
            throw new CommsException("["+token1.getLine()+"] Token1 is not a word "+token1) ;
        String code = token1.getImage() ;
        if ( code.length() != 2 )
            throw new CommsException("["+token1.getLine()+"] Code is not 2 characters "+code) ;

        switch (code) {
            case "QA": {
                if ( line.size() != 4 && line.size() != 5 )
                    throw new CommsException("["+token1.getLine()+"] Quad add tuple error: length = "+line.size()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.size()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.add(g, s, p, o);
                return false ;
            }
            case "QD": {
                if ( line.size() != 4 && line.size() != 5 )
                    throw new CommsException("["+token1.getLine()+"] Quad delete tuple error: length = "+line.size()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.size()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.delete(g, s, p, o);
                return false ;
            }
            case "PA": {
                if ( line.size() != 3 && line.size() != 4 )
                    throw new CommsException("["+token1.getLine()+"] Prefix add tuple error: length = "+line.size()) ;
                String prefix = line.get(1).asString() ;
                String uriStr = line.get(2).asString() ;
                Node gn = line.size()==3 ? null : line.get(3).asNode() ;  
                sink.addPrefix(gn, prefix, uriStr);
                return false ;
            }
            case "PD": {
                if ( line.size() != 2 && line.size() != 3 )
                    throw new CommsException("["+token1.getLine()+"] Prefix delete tuple error: length = "+line.size()) ;
                String prefix = line.get(1).asString() ;
                Node gn = line.size()==2 ? null : line.get(3).asNode() ;  
                sink.deletePrefix(gn, prefix);
                return false ;
            }
            case "TB": {
                sink.txnBegin(ReadWrite.WRITE);
                return false ;
            }
            case "TC": {
                // Possible return
                sink.txnCommit();
                return true ;
            }
            case "TA": {
                // Possible return
                sink.txnAbort();
                return true ;
            }
            default:  {
                throw new CommsException("["+token1.getLine()+"] Code '"+code+"' not recognized") ;
            }
        }
    }

    private static Node tokenToNode(Token token) {
        if ( token.isIRI() )
            return RiotLib.createIRIorBNode(token.getImage()) ;
        return token.asNode() ;
    }
    


}

