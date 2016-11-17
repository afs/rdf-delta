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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.Tokenizer ;
import org.apache.jena.riot.tokens.TokenizerFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestTokenizerTextOther extends AbstractTestTokenizerOther {

    @Override
    protected Tokenizer tokenizer(InputStream input, boolean lineMode) {
        return TokenizerFactory.makeTokenizerUTF8(input);
    }
    
    @Test
    public void tokenizer_charset_1() {
        ByteArrayInputStream in = bytes("'abc'");
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
        Token t = tokenizer.next();
        Assert.assertFalse(tokenizer.hasNext());
    }

    @Test(expected = RiotParseException.class)
    public void tokenizer_charset_2() {
        ByteArrayInputStream in = bytes("'abcdé'");
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
        Token t = tokenizer.next();
        Assert.assertFalse(tokenizer.hasNext());
    }

    @Test(expected = RiotParseException.class)
    public void tokenizer_charset_3() {
        ByteArrayInputStream in = bytes("<http://example/abcdé>");
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
        Token t = tokenizer.next();
        Assert.assertFalse(tokenizer.hasNext());
    }


}
