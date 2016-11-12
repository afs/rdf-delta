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

import static org.apache.jena.atlas.lib.Chars.CH_LT;
import static org.apache.jena.atlas.lib.Chars.CH_RSLASH;
import static org.apache.jena.atlas.lib.Chars.SPC;
import static org.apache.jena.atlas.lib.Chars.TAB;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.jena.atlas.AtlasException;
import org.apache.jena.atlas.lib.EscapeStr;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;

public class TIOParserBase {
    
    private static final boolean AllowSpacesInIRI = false;
    private List<Token> current = new ArrayList<>();
    private Consumer<Tuple<Token>> tupleDest = null;
    private Consumer<Token> tokenDest = null;
    
    // DRY Share with TokenizerText
    private static class ErrorHandlerTokenizer implements ErrorHandler {
        @Override public void warning(String message, long line, long col) {
            // Warning/continue.
            //ErrorHandlerFactory.errorHandlerStd.warning(message, line, col);
            throw new RiotParseException(message, line, col) ;
        }

        @Override public void error(String message, long line, long col) {
            throw new RiotParseException(message, line, col) ;
        }

        @Override public void fatal(String message, long line, long col) {
            throw new RiotParseException(message, line, col) ;
        }
    } ;
    // The code assumes that errors throw exception and so stop parsing.
    private static final ErrorHandler defaultErrorHandler = new ErrorHandlerTokenizer() ;
    private ErrorHandler errorHandler = defaultErrorHandler ;

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(ErrorHandler handler) {
        this.errorHandler = handler;
    }

    public TIOParserBase() {}
    
    public void startTuples()  {
        if ( tupleDest == null )
            throw new RiotException("No tuples handler");
        this.tokenDest = (t) -> current.add(t);
    }
    
    public void finishTuples() {
    }

    public void startTokens()  {
        if ( tokenDest == null )
            throw new RiotException("No tokens handler");
    }
    
    public void finishTokens() {
    }
    
    public void setTokenDest(Consumer<Token> dest) {
        this.tokenDest = dest;
    }
    
    public void setTupleDest(Consumer<Tuple<Token>> dest) {
        this.tupleDest = dest;
        // And set the token handling to be to accumulate a line.
        this.tokenDest = (t) -> current.add(t);
    }
    
    protected void startTuple() { current.clear(); }
    
    protected void finishTuple(int line, int column) {
        Tuple<Token> x = TupleFactory.create(current);
        tupleDest.accept(x);
    }

    protected void emitKeyword(String image, int beginLine, int beginColumn) {
        Token t = Token.tokenForWord(image);
        emit(t);
    }
    
    protected void emitSymbol(TokenType tt, int line, int column) {
       Token t = Token.tokenForWord("");
       t.setImage(null);
       t.setType(tt);
       emit(t);
    }

    protected void emitANY(String image, int beginLine, int beginColumn) {
        emitKeyword(image, beginLine, beginColumn);
    }
    
    private void emit(Token t) {
        tokenDest.accept(t);
    }

    protected void emitIRI(String uristr) {
        Token t = new Token(TokenType.IRI, uristr);
        emit(t);
    }
    
    protected void emitPrefixedName(String pname) {
        // @@ Unescape rules.
        int idx = pname.indexOf(':');
        String prefix = pname.substring(0, idx);
        String localname = pname.substring(idx+1);
        // Syntactically legal at this point so not checking needed. 
        localname = processPN_LOCAL_ESC(localname);
        Token t = new Token(TokenType.PREFIXED_NAME, prefix, localname);
        emit(t);
    }
    
    private String processPN_LOCAL_ESC(String str) {
        if ( str.indexOf('\\') < 0 )
            return str ;
        return str.replace("\\", "") ;
    }
    
    protected void emitBoolean(String lex, int line, int column) {
        emitLiteral(lex, null, XSDDatatype.XSDboolean.getURI());
    }
    
    protected void emitLiteralInteger(String lexicalForm, int line, int column) {
        Token t = new Token(TokenType.INTEGER, lexicalForm);
        emit(t);
    }

    protected void emitLiteralDecimal(String lexicalForm, int line, int column) {
        Token t = new Token(TokenType.DECIMAL, lexicalForm);
        emit(t);
    }

    protected void emitLiteralDouble(String lexicalForm, int line, int column) {
        Token t = new Token(TokenType.DOUBLE, lexicalForm);
        emit(t);
    }

    protected void emitLiteral(String lexicalForm, String langTag, String datatypeURI) { //, int line, int column) {
        //String type?
        Token token = new Token(TokenType.STRING, lexicalForm);

        if ( langTag != null ) {
            Token mainToken = new Token(token) ;
            mainToken.setType(TokenType.LITERAL_LANG) ;
            mainToken.setSubToken1(token) ;
            mainToken.setImage2(langTag) ;
            token = mainToken ;
            emit(token);
            return ;
        }

        if ( datatypeURI != null ) {
            Token mainToken = new Token(token) ;
            // @@ May be PrefixedName.
            Token subToken = new Token(TokenType.IRI, datatypeURI);
            mainToken.setSubToken1(token) ;
            mainToken.setSubToken2(subToken) ;
            mainToken.setType(TokenType.LITERAL_DT) ;
            mainToken.setImage(token.getImage()) ;
            emit(mainToken);
            return;
        }
        // String.
        emit(token);
        
    }
    
    protected void emitHex(String hexStr, int line, int column) {
        Token token = new Token(TokenType.HEX, hexStr);
        emit(token);
    }
    
    protected void emitVariable(String varName, int line, int column) {
        Token t = new Token(TokenType.VAR, varName); 
        emit(t);
    }
    
    protected void emitDirective(String image, int line, int column) {
        Token t = new Token(TokenType.DIRECTIVE, image); 
        emit(t);
    }
    
    public static String unescapeStr(String s)
    { return unescape(s, '\\', false, 1, 1); }

//    public static String unescapeCodePoint(String s)
//    { return unescape(s, '\\', true, 1, 1); }
//
//    protected String unescapeCodePoint(String s, int line, int column)
//    { return unescape(s, '\\', true, line, column); }

    // The javacc tokenizer handles "<horizontal>" and then thsi check it further for the '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
    // This makes it as like TokenizerText as possible.
    protected String processIRI(String iriStr, int line, int column) {
        iriStr = stripQuotes(iriStr);
        iriStr = unescape(iriStr, '\\', true, line, column);
        int N = iriStr.length();
        for(int i = 0 ; i < N ; i++ ) {
            char ch = iriStr.charAt(i);
            // SeeTokenizerText
            switch(ch) {
                case CH_LT:
                    // Probably a corrupt file so not a warning.
                    badCharInIRI(line, column, "Bad character in IRI (bad character: '<'): <%s[<]...>", iriStr.substring(0, i)) ;
                    break;
                case TAB:
                    badCharInIRI(line, column, "Bad character in IRI (Tab character): <%s[tab]...>", iriStr.substring(0, i)) ;
                    break;
                case CH_RSLASH:
                    // Already unescaped.
                    badCharInIRI(line, column, "Bad escape sequence in IRI: <%s\\...>", iriStr.substring(0, i)) ;
                    break ;                    
                case '{': case '}': case '"': case '|': case '^': case '`' :
                    badCharInIRI(line, column, "Illegal character in IRI (codepoint 0x%02X, '%c'): <%s[%c]...>", (int)ch, ch, iriStr.substring(0, i), ch) ;
//                    if ( ! VeryVeryLaxIRI )
//                            warning("Illegal character in IRI (codepoint 0x%02X, '%c'): <%s[%c]...>", ch, (char)ch, iriStr.substring(0, i), (char)ch) ;
//                    break ;
                    break;
                case SPC:
                    if ( ! AllowSpacesInIRI )
                        badCharInIRI(line, column, "Bad character in IRI (space): <%s[space]...>", iriStr.substring(0, i)) ;
                    break ;
                default:
                    if ( ch <= 0x19 )
                        badCharInIRI(line, column, "Illegal character in IRI (control char 0x%02X): <%s[0x%02X]...>", (int)ch, iriStr.substring(0, i), (int)ch) ;
            }
        }
        
        return iriStr;
    }
    
    // Do we need the line/column versions?  
    // Why not catch exceptions and comvert to  QueryParseException
    
    private void badCharInIRI(int line, int column, String fmtstring, Object...args) {
        errorHandler.warning(String.format(fmtstring, args), line, column);
    }

    public static String unescapeStr(String s, int line, int column)
    { return unescape(s, '\\', false, line, column); }
    
    // Worker function
    public static String unescape(String s, char escape, boolean pointCodeOnly, int line, int column) {
        try {
            return EscapeStr.unescape(s, escape, pointCodeOnly);
        } catch (AtlasException ex) {
            throw new RiotParseException(ex.getMessage(), line, column);
        }
    }

    // Unlabelled bNode.
    protected void emitNewBlankNode(int line, int column) {
        String label = genLabel();
        Token t = new Token(TokenType.BNODE, label);
        emit(t);
    }

    // Labelled bNode.
    protected void emitBlankNode(String label, int line, int column) {
        if ( label.startsWith("_:") )
            label = label.substring("_:".length());
        if ( label.isEmpty() )
            throw new RiotParseException("No label for blank node", line, column);
        Token t = new Token(TokenType.BNODE, label);
        emit(t);
    }

    private String genLabel() {
        return UUID.randomUUID().toString();
    }

    /** Remove first and last characters (e.g. ' or "") from a string */
    protected static String stripQuotes(String s) {
        return s.substring(1, s.length() - 1);
    }

    /** Remove first 3 and last 3 characters (e.g. ''' or """) from a string */
    protected static String stripQuotes3(String s) {
        return s.substring(3, s.length() - 3);
    }

    /** remove the first n charcacters from the string */
    public static String stripChars(String s, int n) {
        return s.substring(n, s.length());
    }

}
