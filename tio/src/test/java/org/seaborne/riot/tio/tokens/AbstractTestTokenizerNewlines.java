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

import static org.junit.Assert.assertFalse ;

import org.apache.jena.riot.tokens.TokenType ;
import org.apache.jena.riot.tokens.Tokenizer ;
import org.junit.Test ;

/** Newline handling.  Not all tokenizers offer this */ 
public abstract class AbstractTestTokenizerNewlines extends BaseTestTokenizer {

    @Test
    public void token_newlines_1() {
        Tokenizer tokenizer = tokenizer("\n", true) ;
        testNextToken(tokenizer, TokenType.NL) ;
        assertFalse(tokenizer.hasNext()) ;
    }

    @Test
    public void token_newlines_2() {
        Tokenizer tokenizer = tokenizer("abc\ndef", true) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "abc") ;
        testNextToken(tokenizer, TokenType.NL) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "def") ;
        assertFalse(tokenizer.hasNext()) ;
    }

    @Test
    public void token_newlines_3() {
        Tokenizer tokenizer = tokenizer("abc\n\ndef", true) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "abc") ;
        testNextToken(tokenizer, TokenType.NL) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "def") ;
        assertFalse(tokenizer.hasNext()) ;
    }

    @Test
    public void token_newlines_4() {
        Tokenizer tokenizer = tokenizer("abc\n\rdef", true) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "abc") ;
        testNextToken(tokenizer, TokenType.NL) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "def") ;
        assertFalse(tokenizer.hasNext()) ;
    }

    public void token_newlines_5() {
        Tokenizer tokenizer = tokenizer("abc\n\n", true) ;
        testNextToken(tokenizer, TokenType.KEYWORD, "abc") ;
        testNextToken(tokenizer, TokenType.NL) ;
        assertFalse(tokenizer.hasNext()) ;
    }

    public void token_newlines_6() {
        Tokenizer tokenizer = tokenizer("\n \n", true) ;
        testNextToken(tokenizer, TokenType.NL) ;
        testNextToken(tokenizer, TokenType.NL) ;
        assertFalse(tokenizer.hasNext()) ;
    }
}
