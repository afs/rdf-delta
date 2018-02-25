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

package org.seaborne.patch.text;

import static org.apache.jena.riot.tokens.TokenType.DOT;
import static org.seaborne.patch.changes.PatchCodes.ADD_DATA;
import static org.seaborne.patch.changes.PatchCodes.ADD_PREFIX;
import static org.seaborne.patch.changes.PatchCodes.DEL_DATA;
import static org.seaborne.patch.changes.PatchCodes.DEL_PREFIX;
import static org.seaborne.patch.changes.PatchCodes.HEADER;
import static org.seaborne.patch.changes.PatchCodes.SEGMENT;
import static org.seaborne.patch.changes.PatchCodes.TXN_ABORT;
import static org.seaborne.patch.changes.PatchCodes.TXN_BEGIN;
import static org.seaborne.patch.changes.PatchCodes.TXN_COMMIT;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.SysRIOT;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerFactory;
import org.seaborne.patch.PatchException;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.PatchProcessor;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.changes.PatchCodes;

// Replaces RDFPatchReaderText
// This reader direct consumes the token stream, not create tuples. 
public class RDFPatchReaderText implements PatchProcessor {
    private final Tokenizer tokenizer;
    
    
    // Return true on end of transaction.
    private static void read(Tokenizer tokenizer, RDFChanges changes) {
        while( tokenizer.hasNext() ) {
            apply1(tokenizer, changes);
        }
    }
    
    public RDFPatchReaderText(InputStream input) {
        tokenizer = TokenizerFactory.makeTokenizerUTF8(input);
    }
    
    @Override
    public void apply(RDFChanges processor) {
        read(tokenizer, processor);
    }

    /** Execute one tuple, skipping blanks and comments.
     *  Return true if there is the possiblity of more.
     */
    private static boolean apply1(Tokenizer input, RDFChanges sink) {
        boolean oneTransaction = true;  
        long lineNumber = 0 ;
        while(input.hasNext()) {
            try {
                lineNumber++;
                boolean b = doOneLine(input, sink);
                if ( oneTransaction && b )
                    return true;
            } catch (Exception ex) {
                ex.printStackTrace(System.err);
                sink.txnAbort();
                return false;
            }
        }
        return false;
    }
    
    // Return true for "end transaction".
    private static boolean doOneLine(Tokenizer tokenizer, RDFChanges sink) {
        if ( !tokenizer.hasNext() )
            return false;
        Token tokCode = tokenizer.next();
        if ( tokCode.hasType(DOT) )
            throw exception(tokCode, "Empty line");
        if ( ! tokCode.isWord() )
            throw exception(tokCode, "Expected keyword at start of patch record");
        
        String code = tokCode.getImage();
        switch (code) {
            case HEADER: {
                readHeaderLine(tokenizer, (f,v)->sink.header(f, v));
                return false;
            }

            case ADD_DATA: {
                Node s = nextNode(tokenizer);
                Node p = nextNode(tokenizer);
                Node o = nextNode(tokenizer);
                Node g = nextNodeMaybe(tokenizer);
                skip(tokenizer, DOT);
                sink.add(g, s, p, o);
                return false;
            }
            case DEL_DATA: {
                Node s = nextNode(tokenizer);
                Node p = nextNode(tokenizer);
                Node o = nextNode(tokenizer);
                Node g = nextNodeMaybe(tokenizer);
                skip(tokenizer, DOT);
                sink.delete(g, s, p, o);
                return false;
            }
            case ADD_PREFIX: {
                Token tokPrefix = nextToken(tokenizer); 
                if ( tokPrefix == null )
                    throw new PatchException("["+tokCode.getLine()+"] Prefix add tuple too short");
                String prefix = tokPrefix.asString();
                if ( prefix == null )
                    throw exception(tokPrefix, "Prefix is not a string: %s", tokPrefix); 
                String uriStr;
                Token tokURI = nextToken(tokenizer);
                if ( tokURI.isIRI() )
                    uriStr = tokURI.getImage();
                else if ( tokURI.isString() )
                    uriStr = tokURI.asString();
                else
                    throw exception(tokURI, "Prefix error: URI slot is not a URI nor a string");
                Node gn = nextNodeMaybe(tokenizer);
                skip(tokenizer, DOT);
                sink.addPrefix(gn, prefix, uriStr);
                return false;
            }
            case DEL_PREFIX: {
                Token tokPrefix = nextToken(tokenizer); 
                if ( tokPrefix == null )
                    throw new PatchException("["+tokCode.getLine()+"] Prefix delete tuple too short");
                String prefix = tokPrefix.asString();
                if ( prefix == null )
                    throw exception(tokPrefix, "Prefix is not a string: %s", tokPrefix); 
                Node gn = nextNodeMaybe(tokenizer);
                skip(tokenizer, DOT);
                sink.deletePrefix(gn, prefix);
                return false;
            }
            case TXN_BEGIN: 
                // Alternative name:
            case "TB": {
                skip(tokenizer, DOT);
                sink.txnBegin();
                return false;
            }
            case TXN_COMMIT: {
                skip(tokenizer, DOT);
                sink.txnCommit();
                return true;
            }
            case TXN_ABORT: {
                skip(tokenizer, DOT);
                sink.txnAbort();
                return true;
            }
            case SEGMENT: {
                skip(tokenizer, DOT);
                sink.segment();
                return false;
            }
            default:  {
                throw new PatchException("["+tokCode.getLine()+"] Code '"+code+"' not recognized");
            }
        }
    }
        
    private final static String bNodeLabelStart = "_:";
    private static Node tokenToNode(Token token) {
        if ( token.isIRI() )
            // URI or <_:...>
            return RiotLib.createIRIorBNode(token.getImage());
        if ( token.isBNode() ) {
            // Blank node as _:...
            String label = token.getImage().substring(bNodeLabelStart.length());
            return NodeFactory.createBlankNode(label);
        }
        Node node = token.asNode();
        if ( node == null ) 
            throw exception(token, "Expect a Node, got %s",token);
        return node;
    }
    
    /** Read patch header. */
    public static PatchHeader readerHeader(InputStream input) {
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerUTF8(input);
        Map<String, Node> header = new LinkedHashMap<>();
        int lineNumber = 0;
        while(tokenizer.hasNext()) {
          Token tokCode = tokenizer.next();
          if ( tokCode.hasType(DOT) )
              throw exception(tokCode, "Empty header line");
          if ( ! tokCode.isWord() )
              throw exception(tokCode, "Expected keyword at start of patch header");
          String code = tokCode.getImage();
          lineNumber ++;
          if ( ! code.equals(PatchCodes.HEADER) )
              break;
          readHeaderLine(tokenizer, (f,n)->header.put(f, n));
      }
      return new PatchHeader(header);
    }
    
    /** Known-to-be-header line */
    private static void readHeaderLine(Tokenizer tokenizer, BiConsumer<String, Node> action) {
        Token token2 = nextToken(tokenizer);
        if ( ! token2.isWord() && ! token2.isString() )
            throw new PatchException("["+token2.getLine()+"] Header does not have a key that is a word: "+token2);
        String field = token2.getImage(); 
        Node v = nextNode(tokenizer);
        skip(tokenizer, DOT);
        action.accept(field, v);
    }

    
    private static void skip(Tokenizer tokenizer, TokenType tokenType ) {
        Token tok = tokenizer.next();
        if ( ! tok.hasType(tokenType) )
            throw exception(tok, "Expected token type: "+tokenType+": got "+tok);
    }

    private static Node nextNodeMaybe(Tokenizer tokenizer) {
        Token tok = tokenizer.peek();
        if ( tok.hasType(DOT) )
            return null;
        if ( tok.isEOF() )
            throw new PatchException("Input truncated: no DOT seen on last line");
        return tokenToNode(tokenizer.next());
    }

    // Next token, required, must not be EOF or DOT.
    private static Token nextToken(Tokenizer tokenizer) {
        Token tok = tokenizer.next();
        if ( tok.hasType(DOT) )
            throw exception(tok, "Input truncated by DOT: line too short");
        if ( tok.isEOF() )
            throw new PatchException("Input truncated: no DOT seen on last line");
        return tok;
    } 
    
    private static Node nextNode(Tokenizer tokenizer) {
        return tokenToNode(nextToken(tokenizer));
    }
    
    private static PatchException exception(Token token, String fmt, Object... args) {
        String msg = String.format(fmt, args);
        if ( token != null )
            msg = SysRIOT.fmtMessage(msg, token.getLine(), token.getColumn());    
        return new PatchException(msg);
   }
}
