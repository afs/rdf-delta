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

package org.seaborne.riot.tio.alt;

import java.io.InputStream ;
import java.util.ArrayList ;
import java.util.List ;

import org.apache.jena.atlas.iterator.PeekIterator ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.TokenType ;
import org.apache.jena.riot.tokens.Tokenizer ;

public class TokenizerJavacc implements Tokenizer {

    // Directly connect into the Javacc tokenizer.
    // But we have to reconstruct literals etc.
    
    // Just hook into emit* from TIOParserBase 
    
    static Token tokenEndTuple ;
    static {
        // Hack for DOT
        tokenEndTuple = Token.tokenForWord("") ;
        tokenEndTuple.setImage(null) ;
        tokenEndTuple.setType(TokenType.DOT) ;
    }

    private PeekIterator<Token> iterator ;
    
    // Just a quick hack!
    // Materialize and rebuild a token stream.
    public TokenizerJavacc(InputStream in) {
        TupleReaderJavacc trj = new TupleReaderJavacc(in) ;
        List<Token> tokens = new ArrayList<>() ;
        for ( Tuple<Token> tt : trj ) {
            for ( Token t : tt ) {
                tokens.add(t) ;
            }
            tokens.add(tokenEndTuple) ;
        }
        iterator = PeekIterator.create(tokens.iterator()) ;
    }
    
    @Override
    public boolean hasNext() {
        return iterator.hasNext() ;
    }

    private Token lastToken = null ;
    
    @Override
    public Token next() {
        lastToken = iterator.next() ;
        return lastToken ;
    }

    @Override
    public Token peek() {
        return iterator.peek() ;
    }

    @Override
    public boolean eof() {
        return !hasNext() ;
    }

    @Override
    public long getLine() {
        if ( lastToken != null )
            return lastToken.getLine() ;
        return -1 ;
    }

    @Override
    public long getColumn() {
        if ( lastToken != null )
            return lastToken.getColumn() ;
        return -1 ;
    }

    @Override
    public void close() {}
}
