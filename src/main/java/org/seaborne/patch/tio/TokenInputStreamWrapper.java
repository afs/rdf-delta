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

package org.seaborne.patch.tio;

import java.util.Iterator ;
import java.util.List ;

import org.apache.jena.atlas.lib.Closeable ;
import org.apache.jena.riot.tokens.Token ;

public class TokenInputStreamWrapper implements Iterator<List<Token>>, Iterable<List<Token>>, Closeable
{
    private TokenInputStream stream ;
    
    public TokenInputStreamWrapper(TokenInputStream stream) { this.stream = stream ; }

    @Override
    public boolean hasNext()                    { return stream.hasNext() ; }

    @Override
    public List<Token> next()                   { return stream.next() ; }

    @Override
    public void remove()                        { stream.remove() ; }

    @Override
    public Iterator<List<Token>> iterator()     { return stream.iterator() ; }

    @Override
    public void close()                         { stream.close(); }
}
