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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory ;
import org.apache.jena.riot.system.RiotLib ;
import org.apache.jena.riot.tokens.Token ;
import org.seaborne.riot.tio.TupleIO ;
import org.seaborne.riot.tio.TupleReader ;
import static org.seaborne.patch.changes.PatchCodes.*;

/** PatchReader for text input */ 
// Needs reworking: for efficiency
public class RDFPatchReaderText implements PatchProcessor {
    private final TupleReader input ;
    
    private RDFPatchReaderText(TupleReader in) {
        input = in ;
    }
    
    /** Create a patch reader. */
    public RDFPatchReaderText(InputStream in) {
        input = TupleIO.createTupleReaderText(in) ; 
    }
    
    @Override
    public void apply(RDFChanges processor) {
        read(input, processor);
    }

    private void read(TupleReader input, RDFChanges processor) {
        try {
            while( input.hasNext() ) {
                apply1(input, processor);
            }
        } finally { IO.close(input); }
    }
    
    public static PatchProcessor create(InputStream input) { return new RDFPatchReaderText(input); }

    // -- statics.
    
    /** Execute one tuple, skipping blanks and comments.
     *  Return true if there is the possiblity of more.
     */
    private static boolean apply1(TupleReader input, RDFChanges sink) {
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
    private static boolean doOneLine(Tuple<Token> line, RDFChanges sink) {
        Token token1 = line.get(0) ;
        if ( ! token1.isWord() )
            throw new PatchException("["+token1.getLine()+"] Token1 is not a word "+token1) ;
        String code = token1.getImage() ;
        if ( code.length() > 2 )
            throw new PatchException("["+token1.getLine()+"] Code too long: "+code) ;

        switch (code) {
            case HEADER: {
                readHeaderLine(line, (f,v)->sink.header(f, v));
                return false ;
            }
            
            case ADD_DATA: {
                if ( line.len() != 4 && line.len() != 5 )
                    throw new PatchException("["+token1.getLine()+"] Quad add tuple error: length = "+line.len()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.len()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.add(g, s, p, o);
                return false ;
            }
            case DEL_DATA: {
                if ( line.len() != 4 && line.len() != 5 )
                    throw new PatchException("["+token1.getLine()+"] Quad delete tuple error: length = "+line.len()) ;
                Node s = tokenToNode(line.get(1)) ;
                Node p = tokenToNode(line.get(2)) ;
                Node o = tokenToNode(line.get(3)) ;
                Node g = line.len()==4 ? null : tokenToNode(line.get(4)) ;  
                sink.delete(g, s, p, o);
                return false ;
            }
            case ADD_PREFIX: {
                if ( line.len() != 3 && line.len() != 4 )
                    throw new PatchException("["+token1.getLine()+"] Prefix add tuple error: length = "+line.len()) ;
                String prefix = line.get(1).asString() ;
                String uriStr;
                Token t = line.get(2);
                if ( t.isIRI() )
                    uriStr = t.getImage();
                else if ( t.isString() )
                    uriStr = t.asString();
                else
                    throw new PatchException("["+token1.getLine()+"] Prefix error: URI slot is not a URI nor a string") ;
                Node gn = line.len()==3 ? null : line.get(3).asNode() ;  
                sink.addPrefix(gn, prefix, uriStr);
                return false ;
            }
            case DEL_PREFIX: {
                if ( line.len() != 2 && line.len() != 3 )
                    throw new PatchException("["+token1.getLine()+"] Prefix delete tuple error: length = "+line.len()) ;
                String prefix = line.get(1).asString() ;
                Node gn = line.len()==2 ? null : line.get(3).asNode() ;  
                sink.deletePrefix(gn, prefix);
                return false ;
            }
            case TXN_BEGIN: 
                // Alternative name:
            case "TB": 
            {
                sink.txnBegin();
                return false ;
            }
            case TXN_COMMIT: {
                // Possible return
                sink.txnCommit();
                return true ;
            }
            case TXN_ABORT: {
                // Possible return
                sink.txnAbort();
                return true ;
            }
            case SEGMENT:
                sink.segment();
                return false;
            default:  {
                throw new PatchException("["+token1.getLine()+"] Code '"+code+"' not recognized") ;
            }
        }
    }

    private final static String bNodeLabelStart = "_:";
    private static Node tokenToNode(Token token) {
        if ( token.isIRI() )
            // URI or <_:...>
            return RiotLib.createIRIorBNode(token.getImage()) ;
        if ( token.isBNode() ) {
            // Blank node as _:...
            String label = token.getImage().substring(bNodeLabelStart.length());
            return NodeFactory.createBlankNode(label);
        }
        return token.asNode() ;
    }
    
    /** Read patch header. */
    public static PatchHeader readerHeader(InputStream in) {
        TupleReader input = TupleIO.createTupleReaderText(in) ; 
        return readerHeader(input);
    }

    /** Read the header */
    private static PatchHeader readerHeader(TupleReader input) {
        Map<String, Node> header = new LinkedHashMap<>();
        int lineNumber = 0 ;
        while(input.hasNext()) {
            Tuple<Token> line = input.next() ;
            lineNumber ++ ;
            if ( line.isEmpty() )
                throw new PatchException("["+lineNumber+"] empty line") ;
            Token token1 = line.get(0) ;
            if ( ! token1.isWord() )
                throw new PatchException("["+token1.getLine()+"] Token1 is not a word "+token1) ;
            String code = token1.getImage() ;
            if ( ! code.equals("H") )
                break;
            readHeaderLine(line, (f,n)->header.put(f, n));
        }
        return new PatchHeader(header);
    }

    /** Known-to-be-header line */
    private static void readHeaderLine(Tuple<Token> line, BiConsumer<String, Node> action) {
        if ( line.len() != 3 )
            throw new PatchException("["+line.get(0).getLine()+"] Header: length = "+line.len()) ;
        Token token2 = line.get(1) ;
        if ( ! token2.isWord() && ! token2.isString() )
            throw new PatchException("["+line.get(0).getLine()+"] Header does not have a key that is a word: "+token2) ;
        String field = token2.getImage() ;
        Node v = tokenToNode(line.get(2)) ;
        action.accept(field, v);
    }
}

