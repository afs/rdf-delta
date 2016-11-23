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

import java.io.InputStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.graph.Node ;
import org.apache.jena.riot.system.RiotLib ;
import org.apache.jena.riot.tokens.Token ;
import org.seaborne.riot.tio.TupleIO ;
import org.seaborne.riot.tio.TupleReader ;

// Needs reworking: for efficiency, for less features
/** Must close the input stream */
public class PatchReader implements PatchProcessor {
    private final TupleReader input ;
    
    public PatchReader(TupleReader in) {
        input = in ;
    }
    
    public PatchReader(InputStream in) {
        input = TupleIO.createTupleReaderText(in) ; 
    }

    /** Execute transactions until the input ends or something goes wrong. */
    @Override
    public void apply(RDFChanges sink) {
        try { 
            PatchProcessor.super.apply(sink);
        } finally { 
            IO.close(input) ;
        }
    }
    
    @Override
    public boolean hasMore() {
        return input.hasNext() ;
    }
    
    /** Execute one transaction.
     *  Return true if there is the possiblity of more.
     */
    @Override
    public boolean apply1(RDFChanges sink) {
        // Abort if no end of transaction
        
        boolean oneTransaction = true ;  
        
        int lineNumber = 0 ;
        while(input.hasNext()) {
            Tuple<Token> line = input.next() ;
            //System.err.println("Line = "+line);
            if ( line.isEmpty() )
                throw new PatchException("["+lineNumber+"] empty line") ;
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
        
    // Return true on end of transaction.
    private boolean doOneLine(Tuple<Token> line, RDFChanges sink) {
        Token token1 = line.get(0) ;
        if ( ! token1.isWord() )
            throw new PatchException("["+token1.getLine()+"] Token1 is not a word "+token1) ;
        String code = token1.getImage() ;
        if ( ! code.equals("H") && code.length() != 2 )
            throw new PatchException("["+token1.getLine()+"] Code is header nor 2 characters "+code) ;

        switch (code) {
            case "H": {
                if ( line.len() != 3 )
                    throw new PatchException("["+token1.getLine()+"] Header: length = "+line.len()) ;
                Token token2 = line.get(1) ;
                if ( ! token2.isWord() && ! token2.isString() )
                    throw new PatchException("["+token1.getLine()+"] Header doesnot what with a word: "+token2) ;
                String field = line.get(1).getImage() ;
                Node v = tokenToNode(line.get(2)) ;
                sink.header(field, v);
                return false ;
            }
            
            case "QA": {
                if ( line.len() != 4 && line.len() != 5 )
                    throw new PatchException("["+token1.getLine()+"] Quad add tuple error: length = "+line.len()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.len()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.add(g, s, p, o);
                return false ;
            }
            case "QD": {
                if ( line.len() != 4 && line.len() != 5 )
                    throw new PatchException("["+token1.getLine()+"] Quad delete tuple error: length = "+line.len()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.len()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.delete(g, s, p, o);
                return false ;
            }
            case "PA": {
                if ( line.len() != 3 && line.len() != 4 )
                    throw new PatchException("["+token1.getLine()+"] Prefix add tuple error: length = "+line.len()) ;
                String prefix = line.get(1).asString() ;
                String uriStr = line.get(2).asString() ;
                Node gn = line.len()==3 ? null : line.get(3).asNode() ;  
                sink.addPrefix(gn, prefix, uriStr);
                return false ;
            }
            case "PD": {
                if ( line.len() != 2 && line.len() != 3 )
                    throw new PatchException("["+token1.getLine()+"] Prefix delete tuple error: length = "+line.len()) ;
                String prefix = line.get(1).asString() ;
                Node gn = line.len()==2 ? null : line.get(3).asNode() ;  
                sink.deletePrefix(gn, prefix);
                return false ;
            }
            case "TB": {
                sink.txnBegin();
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
                throw new PatchException("["+token1.getLine()+"] Code '"+code+"' not recognized") ;
            }
        }
    }

    private static Node tokenToNode(Token token) {
        if ( token.isIRI() )
            return RiotLib.createIRIorBNode(token.getImage()) ;
        return token.asNode() ;
    }
    


}

