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

package org.seaborne.riot.tio.tokens ;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse ;
import static org.junit.Assert.assertNotNull ;
import static org.junit.Assert.assertTrue ;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream ;
import java.io.InputStream;

import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.TokenType ;
import org.apache.jena.riot.tokens.Tokenizer ;

/** Machinery for {@link Tokenizer} testing. */
public abstract class BaseTestTokenizer {
    
    protected final Tokenizer tokenizer(String string, boolean lineMode) {
        ByteArrayInputStream in = bytes(string) ;
        return tokenizer(in, lineMode) ;
    }
    
    // Implement this !
    protected abstract Tokenizer tokenizer(InputStream input, boolean lineMode);

    protected Tokenizer tokenizer(String string) {
        return tokenizer(string, false) ;
    }

    protected static void assertEqualsToken(Token expected, Token actual) {
        if ( expected.isString() ) {
            if ( ! actual.isString() )
                fail("Not a STRING: "+actual);
            assertEquals(expected.getImage(), actual.getImage()) ;
            return ;
        }
        assertEquals(expected, actual);
    }
    
    protected static void assertEqualsTokenType(TokenType expected, TokenType actual) {
        if ( isString(expected) ) {
            if ( ! isString(actual) )
                fail("Not a STRING: "+actual);
            return; 
        }
        assertEquals(expected, actual);
    }

    private static boolean isString(TokenType tokenType) {
        switch(tokenType) {
            case STRING: case STRING1: case STRING2:
            case LONG_STRING1: case LONG_STRING2:
                return true ;
            default:
                return false ;
        }
    }
    
    protected void tokenFirst(String string) {
        Tokenizer tokenizer = tokenizer(string) ;
        assertTrue(tokenizer.hasNext()) ;
        assertNotNull(tokenizer.next()) ;
        // Maybe more.
        // assertFalse(tokenizer.hasNext()) ;
    }

    protected Token tokenFor(String string) {
        Tokenizer tokenizer = tokenizer(string) ;
        assertTrue(tokenizer.hasNext()) ;
        Token token = tokenizer.next() ;
        assertFalse(tokenizer.hasNext()) ;
        return token ;
    }

    protected Token tokenizeAndTestExact(String input, TokenType tokenType, String tokenImage) {
        return tokenizeAndTestExact(input, tokenType, tokenImage, null) ;
    }

    protected Token tokenizeAndTestExact(String input, TokenType tokenType, String tokenImage1, String tokenImage2) {
        Tokenizer tokenizer = tokenizer(input) ;
        Token token = testNextToken(tokenizer, tokenType, tokenImage1, tokenImage2) ;
        assertFalse("Excess tokens", tokenizer.hasNext()) ;
        return token ;
    }

    protected Token tokenizeAndTestExact(String input, TokenType tokenType, String tokenImage1,
                                              String tokenImage2, Token subToken1, Token subToken2) {
        Token token = tokenFor(input) ;
        assertEqualsTokenType(tokenType, token.getType()) ;
        assertEquals(tokenImage1, token.getImage()) ;
        assertEquals(tokenImage2, token.getImage2()) ;
        assertEqualsToken(subToken1, token.getSubToken1()) ;
        assertEqualsToken(subToken2, token.getSubToken2()) ;
        return token ;
    }

    protected Tokenizer tokenizeAndTestFirst(String input, TokenType tokenType, String tokenImage) {
        return tokenizeAndTestFirst(input, tokenType, tokenImage, null) ;
    }

    protected Tokenizer tokenizeAndTestFirst(String input, TokenType tokenType, String tokenImage1,
                                                  String tokenImage2) {
        Tokenizer tokenizer = tokenizer(input) ;
        testNextToken(tokenizer, tokenType, tokenImage1, tokenImage2) ;
        return tokenizer ;
    }

    protected static Token testNextToken(Tokenizer tokenizer, TokenType tokenType) {
        return testNextToken(tokenizer, tokenType, null, null) ;
    }

    protected static Token testNextToken(Tokenizer tokenizer, TokenType tokenType, String tokenImage1) {
        return testNextToken(tokenizer, tokenType, tokenImage1, null) ;
    }

    protected static Token testNextToken(Tokenizer tokenizer, TokenType tokenType, String tokenImage1, String tokenImage2) {
        assertTrue(tokenizer.hasNext()) ;
        Token token = tokenizer.next() ;
        assertNotNull(token) ;
        assertEqualsTokenType(tokenType, token.getType()) ;
        assertEquals(tokenImage1, token.getImage()) ;
        assertEquals(tokenImage2, token.getImage2()) ;
        return token ;
    }

    protected Token tokenizeAndTest(String input, TokenType tokenType, String tokenImage1, String tokenImage2,
                                         Token subToken1, Token subToken2) {
        Token token = tokenFor(input) ;
        assertNotNull(token) ;
        assertEqualsTokenType(tokenType, token.getType()) ;
        assertEquals(tokenImage1, token.getImage()) ;
        assertEquals(tokenImage2, token.getImage2()) ;
        assertEqualsToken(subToken1, token.getSubToken1()) ;
        assertEqualsToken(subToken2, token.getSubToken2()) ;
        return token ;
    }

    protected static ByteArrayInputStream bytes(String string) {
        byte b[] = StrUtils.asUTF8bytes(string) ;
        return new ByteArrayInputStream(b) ;
    }

    // First symbol from the stream.
    protected void testSymbol(String string, TokenType expected) {
        tokenizeAndTestFirst(string, expected, null) ;
    }
}
