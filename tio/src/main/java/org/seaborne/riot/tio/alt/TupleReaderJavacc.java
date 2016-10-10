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
import java.util.Iterator ;

import org.apache.jena.atlas.lib.Closeable ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.riot.tokens.Token ;
import org.seaborne.riot.tio.alt.javacc.ParseException ;
import org.seaborne.riot.tio.alt.javacc.TIOjavacc ;

/**
 * TupleReader using Javacc as the parser 
 */
public class TupleReaderJavacc implements Iterator<Tuple<Token>>, Iterable<Tuple<Token>>, Closeable {

    public TupleReaderJavacc(InputStream in) {
        TIOjavacc j = new TIOjavacc(in) ;
        try {
            j.Unit();
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public boolean hasNext() {
        return false ;
    }

    @Override
    public Tuple<Token> next() {
        return null ;
    }

    @Override
    public Iterator<Tuple<Token>> iterator() {
        return this ;
    }

    @Override
    public void close() {}
}
