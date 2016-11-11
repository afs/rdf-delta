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

package org.seaborne.riot.tio;

import static org.apache.jena.riot.tokens.Token.tokenForChar;
import static org.apache.jena.riot.tokens.Token.tokenForNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.jena.atlas.lib.Chars;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Test;
import org.seaborne.riot.tio.impl.TupleReaderTokenizer;
import org.slf4j.impl.StaticLoggerBinder;

public abstract class AbstractTestTokenIO
{
    static { 
        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
        final String clsName = binder.getLoggerFactoryClassStr();
        if ( clsName.contains("JDK14LoggerFactory") )
            LogCtl.setJavaLoggingDft(); 
        else if ( clsName.contains("Log4jLoggerFactory") )
            LogCtl.setLog4j(); 
    }
    
    static Node n1 = SSE.parseNode("<x>");
    static Node n2 = SSE.parseNode("<http://example/y>");
    static Node n3 = SSE.parseNode("<http://example/z>");
    
    static Node n4 = SSE.parseNode("'literal'");
    static Node n5 = SSE.parseNode("'literal'@en");
    static Node n6 = SSE.parseNode("123");
    
    static Node n7 = SSE.parseNode("'123'^^<http://example/myType>");

    private ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    private TokenWriter out = getTokenWriter(bytesOut);

    protected abstract Tokenizer getTokenizer(InputStream in);

    protected abstract TokenWriter getTokenWriter(OutputStream out);

    @Test public void tokens1() {
        send(n7);
        expect(tokenForNode(n7));
    }

    @Test public void tokens2() {
        out.startTuple();
        send(n7);
        out.endTuple();
        out.flush();
        expect(tokenForNode(n7), tokenForChar(Chars.CH_DOT));
    }

    @Test public void tokens3() {
        send(n1, n2, n3, n4, n5, n6, n7);
        
        // Because tokenForNode sets the type as TokenType.STRING
        Token t4 = tokenForNode(n4);
        t4.setType(TokenType.STRING2);
        
        expect(tokenForNode(n1),
               tokenForNode(n2),
               tokenForNode(n3),
               t4,
               tokenForNode(n5),
               tokenForNode(n6),
               tokenForNode(n7));
    }
    
    @Test public void comms1() {
        send(n1);
        expect(n1);
    }

    @Test public void comms2() {
        send(n1, n2, n3);
        expect(n1, n2, n3);
    }

    @Test public void token1() {
        Token[] t = { Token.tokenForWord("WORD1") ,
                      Token.tokenForWord("WORD2") ,
                      Token.tokenForInteger(456) ,
                      Token.tokenForChar(Chars.CH_DOT),
                      Token.tokenForChar(Chars.CH_RBRACE),
                      Token.tokenForChar(Chars.CH_LBRACE),
                      Token.tokenForChar(Chars.CH_RBRACKET),
                      Token.tokenForChar(Chars.CH_LBRACKET),
                      Token.tokenForChar(Chars.CH_RPAREN) ,
                      Token.tokenForChar(Chars.CH_LPAREN) ,
                      Token.tokenForChar(Chars.CH_DOT),
                      Token.tokenForChar(Chars.CH_DOT)
                    };
        
        send(t);
        expect(t); 
    }
    
    private void send(Node... nodes) {
        for ( Node n : nodes )
            out.sendNode(n);
        out.flush();
    }

    private void send(Token... tokens) {
        for ( Token token : tokens )
            out.sendToken(token);
        out.flush();
    }


    
    private void expect(Node... nodes) {
        byte b[] = bytesOut.toByteArray();
//        String s = new String(b, StandardCharsets.UTF_8);
//        System.err.println(">>"+s+"<<");
        Tokenizer tokenizer = getTokenizer(new ByteArrayInputStream(b));
        // tokenizer = new PrintTokenizer("Read", tokenizer);
        TupleReader in = new TupleReaderTokenizer(tokenizer);

        int idx = 0;
        while (in.hasNext()) {
            Tuple<Token> tokens = in.next();
            for ( Token t : tokens ) {
                Node n = nodes[idx++];
                assertEquals(n, t.asNode());
            }
        }
        tokenizer.close();
    }

    private void expect(Token... tokens) {
        byte b[] = bytesOut.toByteArray();
//        String s = new String(b, StandardCharsets.UTF_8);
//        System.err.println(">>"+s+"<<");
        Tokenizer tokenizer = getTokenizer(new ByteArrayInputStream(b));
        for ( Token t : tokens ) {
            Token t2 = tokenizer.next();
            // Strings, regardless.
            if ( t.isString() )
                assertTrue(t2.isString());
            else
                assertEquals(t, t2);
        }

        assertFalse("Remaining tokens", tokenizer.hasNext());
        tokenizer.close();
    }
}
