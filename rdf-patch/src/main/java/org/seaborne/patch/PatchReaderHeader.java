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

import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.graph.Node ;
import org.apache.jena.riot.system.RiotLib ;
import org.apache.jena.riot.tokens.Token ;
import org.seaborne.riot.tio.TupleIO ;
import org.seaborne.riot.tio.TupleReader ;

/** Read header (up until the TB or end of file). */
public class PatchReaderHeader {
    private PatchReaderHeader() {}
    
    public static PatchHeader readerHeader(InputStream in) {
        TupleReader input = TupleIO.createTupleReaderText(in) ; 
        return readerHeader(input);
    }

    /** Read the header */
    public static PatchHeader readerHeader(TupleReader input) {
        Map<String, Node> header = new LinkedHashMap<>();
        int lineNumber = 0 ;
        while(input.hasNext()) {
            Tuple<Token> line = input.next() ;
            lineNumber ++ ;
            if ( line.isEmpty() )
                throw new PatchException("["+lineNumber+"] empty line") ;
            boolean more = readHeaderLine(line, (f,n)->header.put(f, n));
            if ( ! more )
                break;
        }
        return new PatchHeader(header);
    }
    
    
    static boolean readHeaderLine(Tuple<Token> line, BiConsumer<String, Node> action) {
        Token token1 = line.get(0) ;
        if ( line.len() != 3 )
            throw new PatchException("["+token1.getLine()+"] Header: length = "+line.len()) ;
        if ( ! token1.isWord() )
            throw new PatchException("["+token1.getLine()+"] Token1 is not a word "+token1) ;
        String code = token1.getImage() ;
        if ( ! code.equals('H') )
            return false;
        Token token2 = line.get(1) ;
        if ( ! token2.isWord() && ! token2.isString() )
            throw new PatchException("["+token1.getLine()+"] Header does not have a key that is a word: "+token2) ;
        String field = token2.getImage() ;
        Node v = tokenToNode(line.get(2)) ;
        action.accept(field, v);
        return true;
    }

    private static Node tokenToNode(Token token) {
        if ( token.isIRI() )
            return RiotLib.createIRIorBNode(token.getImage()) ;
        return token.asNode() ;
    }
}

