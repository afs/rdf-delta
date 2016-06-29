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

package org.seaborne.delta.tio;

import org.apache.jena.riot.tokens.Token ;

import org.apache.jena.graph.Node ;


public class TokenOutputStreamWrapper implements TokenOutputStream
{
    private TokenOutputStream stream ;

    public TokenOutputStreamWrapper(TokenOutputStream stream)
    {
        this.stream = stream ;
    }
    
    @Override
    public void startTuple()                { stream.startTuple() ; }

    @Override
    public void endTuple()                  { stream.endTuple() ; }

    @Override
    public void sendToken(Token token)      { stream.sendToken(token)  ; }

    @Override
    public void sendControl(char character) { stream.sendControl(character) ; }

    @Override
    public void sendNode(Node node)         { stream.sendNode(node) ; }

    @Override
    public void sendNumber(long number)     { stream.sendNumber(number) ; }

    @Override
    public void sendString(String string)   { stream.sendString(string) ; }

    @Override
    public void sendWord(String word)       { stream.sendWord(word) ; }

    @Override
    public void close()                     { stream.close(); }

    @Override
    public void flush()                     { stream.flush(); }

    @Override
    public void sync()                      { stream.sync() ; }
}
