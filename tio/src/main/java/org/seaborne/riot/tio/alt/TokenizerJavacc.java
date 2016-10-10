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

import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.Tokenizer ;
import org.seaborne.riot.tio.alt.javacc.JavaCharStream ;
import org.seaborne.riot.tio.alt.javacc.TIOjavaccTokenManager ;

public class TokenizerJavacc implements Tokenizer {

    JavaCharStream jj_input_stream ;
    TIOjavaccTokenManager token_source ;
    org.seaborne.riot.tio.alt.javacc.Token token ;
    
    public TokenizerJavacc(InputStream stream) {
        try {
            jj_input_stream = new JavaCharStream(stream, "UTF-8", 1, 1) ;
        }
        catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e) ;
        }
        token_source = new TIOjavaccTokenManager(jj_input_stream) ;
        token = new org.seaborne.riot.tio.alt.javacc.Token() ;
    }
    
    @Override
    public boolean hasNext() {
        return false ;
    }

    @Override
    public Token next() {
        return null ;
    }

    @Override
    public Token peek() {
        return null ;
    }

    @Override
    public boolean eof() {
        return false ;
    }

    @Override
    public long getLine() {
        return 0 ;
    }

    @Override
    public long getColumn() {
        return 0 ;
    }

    @Override
    public void close() {}
}
