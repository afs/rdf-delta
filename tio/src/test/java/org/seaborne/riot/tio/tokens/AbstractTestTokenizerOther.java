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

package org.seaborne.riot.tio.tokens;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;

import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerFactory;
import org.junit.Test;

/*8 Tests for non-terms and for multiple tokens. */
public abstract class AbstractTestTokenizerOther extends BaseTestTokenizer {

    
    // @Test
    // public void tokenUnit_cntrl1()
    // {
    // tokenizeAndTestExact("*S", TokenType.CNTRL, "S");
    // }
    //
    // @Test
    // public void tokenUnit_cntr2()
    // {
    // tokenizeAndTestExact("*SXYZ", TokenType.CNTRL, "SXYZ");
    // }
    //
    // @Test
    // public void tokenUnit_cntrl3()
    // {
    // Tokenizer tokenizer = tokenizer("*S<x>");
    // assertTrue(tokenizer.hasNext());
    // Token token = tokenizer.next();
    // assertNotNull(token);
    // assertEquals(TokenType.CNTRL, token.getType());
    // assertEquals('S', token.getCntrlCode());
    // assertNull(token.getImage());
    // assertNull(token.getImage2());
    //
    // assertTrue(tokenizer.hasNext());
    // Token token2 = tokenizer.next();
    // assertNotNull(token2);
    // assertEquals(TokenType.IRI, token2.getType());
    // assertEquals("x", token2.getImage());
    // assertNull(token2.getImage2());
    // assertFalse(tokenizer.hasNext());
    // }

    @Test
    public void tokenUnit_syntax1() {
        tokenizeAndTestExact(".", TokenType.DOT, null, null);
    }

    @Test
    public void tokenUnit_syntax2() {
        Tokenizer tokenizer = tokenizer(".;,");
        testNextToken(tokenizer, TokenType.DOT);
        testNextToken(tokenizer, TokenType.SEMICOLON);
        testNextToken(tokenizer, TokenType.COMMA);
        assertFalse(tokenizer.hasNext());
    }

    @Test
    public void tokenUnit_25() {
        Tokenizer tokenizer = tokenizeAndTestFirst("123:", TokenType.INTEGER, "123");
        testNextToken(tokenizer, TokenType.PREFIXED_NAME, "", "");
    }


    @Test
    public void directive_1() {
        tokenizeAndTestExact("@prefix", TokenType.DIRECTIVE, "prefix");
    }

    @Test
    public void directive_2() {
        tokenizeAndTestExact("@base", TokenType.DIRECTIVE, "base");
    }

    @Test
    public void directive_3() {
        tokenizeAndTestExact("@whatever", TokenType.DIRECTIVE, "whatever");
    }

    @Test
    public void tokenComment_01() {
        tokenizeAndTestExact("_:123 # Comment", TokenType.BNODE, "123");
    }

    @Test
    public void tokenComment_02() {
        tokenizeAndTestExact("\"foo # Non-Comment\"", TokenType.STRING, "foo # Non-Comment");
    }

    @Test
    public void tokenComment_03() {
        Tokenizer tokenizer = tokenizeAndTestFirst("'foo' # Comment\n'bar'", TokenType.STRING, "foo");
        testNextToken(tokenizer, TokenType.STRING, "bar");
    }

    @Test
    public void tokenWord_01() {
        tokenizeAndTestExact("abc", TokenType.KEYWORD, "abc");
    }

    // Multiple terms

    @Test
    public void token_multiple() {
        Tokenizer tokenizer = tokenizer("<x><y>");
        assertTrue(tokenizer.hasNext());
        Token token = tokenizer.next();
        assertNotNull(token);
        assertEquals(TokenType.IRI, token.getType());
        assertEquals("x", token.getImage());

        assertTrue(tokenizer.hasNext());
        Token token2 = tokenizer.next();
        assertNotNull(token2);
        assertEquals(TokenType.IRI, token2.getType());
        assertEquals("y", token2.getImage());

        assertFalse(tokenizer.hasNext());
    }

//    @Test
//    public void tokenizer_charset_1() {
//        ByteArrayInputStream in = bytes("'abc'");
//        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
//        Token t = tokenizer.next();
//        assertFalse(tokenizer.hasNext());
//    }
//
//    @Test(expected = RiotParseException.class)
//    public void tokenizer_charset_2() {
//        ByteArrayInputStream in = bytes("'abcdé'");
//        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
//        Token t = tokenizer.next();
//        assertFalse(tokenizer.hasNext());
//    }
//
//    @Test(expected = RiotParseException.class)
//    public void tokenizer_charset_3() {
//        ByteArrayInputStream in = bytes("<http://example/abcdé>");
//        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
//        Token t = tokenizer.next();
//        assertFalse(tokenizer.hasNext());
//    }

    @Test
    public void tokenizer_BOM_1() {
        // BOM
        ByteArrayInputStream in = bytes("\uFEFF'abc'");
        Tokenizer tokenizer = tokenizer(in, false);
        assertTrue(tokenizer.hasNext());
        Token token = tokenizer.next();
        assertNotNull(token);
        assertEqualsTokenType(TokenType.STRING, token.getType());
        assertEquals("abc", token.getImage());
        assertFalse(tokenizer.hasNext());
    }

    // -- Symbols
    // CNTRL
    // @Test public void tokenizer_symbol_01() { testSymbol("*", TokenType.STAR)
    //; }
    
    @Test
    public void tokenizer_symbol_01() {
        testSymbol("_", TokenType.UNDERSCORE);
    }
    
    @Test
    public void tokenizer_symbol_02() {
        testSymbol("+", TokenType.PLUS);
    }

    @Test
    public void tokenizer_symbol_03() {
        testSymbol("-", TokenType.MINUS);
    }

    // @Test public void tokenizer_symbol_04() { testSymbol("<", TokenType.LT);
    // }
    @Test
    public void tokenizer_symbol_05() {
        testSymbol(">", TokenType.GT);
    }

    @Test
    public void tokenizer_symbol_06() {
        testSymbol("=", TokenType.EQUALS);
    }

    // @Test public void tokenizer_symbol_07() { testSymbol(">=", TokenType.LE)
    //; }
    // @Test public void tokenizer_symbol_08() { testSymbol("<=", TokenType.GE)
    //; }
    // @Test public void tokenizer_symbol_09() { testSymbol("&&",
    // TokenType.LOGICAL_AND); }
    // @Test public void tokenizer_symbol_10() { testSymbol("||",
    // TokenType.LOGICAL_OR); }
    // @Test public void tokenizer_symbol_11() { testSymbol("&  &",
    // TokenType.AMPHERSAND); }
    // @Test public void tokenizer_symbol_12() { testSymbol("| |",
    // TokenType.VBAR); }

    @Test
    public void tokenUnit_symbol_11() {
        testSymbol("+A", TokenType.PLUS);
    }

    @Test
    public void tokenUnit_symbol_12() {
        Tokenizer tokenizer = tokenizeAndTestFirst("+-", TokenType.PLUS, null);
        testNextToken(tokenizer, TokenType.MINUS);
    }

    @Test
    public void tokenUnit_symbol_13() {
        testSymbol(".", TokenType.DOT);
    }

    @Test
    public void tokenUnit_symbol_14() {
        Tokenizer tokenizer = tokenizeAndTestFirst(".a", TokenType.DOT, null);
        testNextToken(tokenizer, TokenType.KEYWORD, "a");
    }
}
