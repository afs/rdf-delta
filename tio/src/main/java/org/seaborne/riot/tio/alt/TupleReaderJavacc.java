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
import java.util.Iterator ;
import java.util.List ;
import java.util.function.Consumer ;

import org.apache.jena.atlas.lib.Closeable ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.riot.tokens.Token ;
import org.seaborne.riot.tio.alt.javacc.ParseException ;
import org.seaborne.riot.tio.alt.javacc.TIOjavacc ;

/**
 * TupleReader using Javacc as the parser 
 */
public class TupleReaderJavacc implements Iterator<Tuple<Token>>, Iterable<Tuple<Token>>, Closeable {
    
    private List<Tuple<Token>> rows = new ArrayList<>() ;
    private Iterator<Tuple<Token>> iterator ;
    
    public TupleReaderJavacc(InputStream in) {
        
        Consumer<Tuple<Token>> dest = (t) -> rows.add(t) ;
        TIOjavacc j = new TIOjavacc(in) ;
        j.setDest(dest); 
        try {
            j.Unit();
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        iterator = rows.iterator() ;
    }
    
    @Override
    public boolean hasNext() {
        return iterator.hasNext() ;
    }

    @Override
    public Tuple<Token> next() {
        return iterator.next() ;
    }

    @Override
    public Iterator<Tuple<Token>> iterator() {
        return this ;
    }

    @Override
    public void close() {}
}
