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

import java.util.ArrayList ;
import java.util.List ;
import java.util.function.Consumer ;

import org.apache.jena.atlas.AtlasException ;
import org.apache.jena.atlas.lib.EscapeStr ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.atlas.lib.tuple.TupleFactory ;
import org.apache.jena.datatypes.RDFDatatype ;
import org.apache.jena.datatypes.TypeMapper ;
import org.apache.jena.datatypes.xsd.XSDDatatype ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory ;
import org.apache.jena.riot.RiotParseException ;
import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.TokenType ;
import org.apache.jena.sparql.graph.NodeConst ;

public class TIOParserBase {

    // NodeConst
    protected final Node XSD_TRUE       = NodeConst.nodeTrue ;
    protected final Node XSD_FALSE      = NodeConst.nodeFalse ; 
//    
//    protected final Node nRDFtype       = NodeConst.nodeRDFType ;
//    
//    protected final Node nRDFnil        = NodeConst.nodeNil ;
//    protected final Node nRDFfirst      = NodeConst.nodeFirst ;
//    protected final Node nRDFrest       = NodeConst.nodeRest ;
//    
//    protected final Node nRDFsubject    = RDF.Nodes.subject ;
//    protected final Node nRDFpredicate  = RDF.Nodes.predicate ;
//    protected final Node nRDFobject     = RDF.Nodes.object ;
    
    
    private List<Token> current = new ArrayList<>() ;
    private Consumer<Tuple<Token>> dest ;
    private static Token[] blank = new Token[]{} ;

    public TIOParserBase() {}
    
    public void setDest(Consumer<Tuple<Token>> dest) {
        this.dest = dest ;
    }
    
    protected void startTuple() { current.clear() ; }
    
    protected void finishTuple(int line, int column) {
        Tuple<Token> x = TupleFactory.create(current) ;
        dest.accept(x) ;
    }

    protected void emitKeyword(String image, int beginLine, int beginColumn) {
        Token t = Token.tokenForWord(image) ;
        current.add(t) ;
    }
    
    protected void emitTerm(Node node) {
        // Silly - don't node it!
        Token t = Token.tokenForNode(node) ;
        current.add(t) ;
    }

    protected void emitSymbol(TokenType tt) {
       Token t = Token.tokenForWord("") ;
       t.setImage(null) ;
       t.setType(tt) ;
       current.add(t) ;
    }
    
    protected Node createLiteralInteger(String lexicalForm) {
        return NodeFactory.createLiteral(lexicalForm, XSDDatatype.XSDinteger) ;
    }

    protected Node createLiteralDouble(String lexicalForm) {
        return NodeFactory.createLiteral(lexicalForm, XSDDatatype.XSDdouble) ;
    }

    protected Node createLiteralDecimal(String lexicalForm) {
        return NodeFactory.createLiteral(lexicalForm, XSDDatatype.XSDdecimal) ;
    }

    protected Node createLiteral(String lexicalForm, String langTag, String datatypeURI) {
        Node n = null ;
        // Can't have type and lang tag in parsing.
        if ( datatypeURI != null ) {
            RDFDatatype dType = TypeMapper.getInstance().getSafeTypeByName(datatypeURI) ;
            n = NodeFactory.createLiteral(lexicalForm, dType) ;
        } else if ( langTag != null && !langTag.isEmpty() )
            n = NodeFactory.createLiteral(lexicalForm, langTag) ;
        else
            n = NodeFactory.createLiteral(lexicalForm) ;
        return n ;
    }
    
    
    public static String unescapeStr(String s)
    { return unescape(s, '\\', false, 1, 1) ; }

//    public static String unescapeCodePoint(String s)
//    { return unescape(s, '\\', true, 1, 1) ; }
//
//    protected String unescapeCodePoint(String s, int line, int column)
//    { return unescape(s, '\\', true, line, column) ; }

    
    protected String resolveQuotedIRI(String iriStr, int line, int column) {
        iriStr = stripQuotes(iriStr) ;
        return iriStr ;
    }
    
    // Do we need the line/column versions?  
    // Why not catch exceptions and comvert to  QueryParseException
    
    public static String unescapeStr(String s, int line, int column)
    { return unescape(s, '\\', false, line, column) ; }
    
    // Worker function
    public static String unescape(String s, char escape, boolean pointCodeOnly, int line, int column) {
        try {
            return EscapeStr.unescape(s, escape, pointCodeOnly) ;
        } catch (AtlasException ex) {
            throw new RiotParseException(ex.getMessage(), line, column) ;
        }
    }

    // Unlabelled bNode.
    protected Node createBNode(int line, int column) {
        throw new NotImplemented() ;
    }

    // Labelled bNode.
    protected Node createBNode(String label, int line, int column) {
        throw new NotImplemented() ;
    }

    /** Remove first and last characters (e.g. ' or "") from a string */
    protected static String stripQuotes(String s) {
        return s.substring(1, s.length() - 1) ;
    }

    /** Remove first 3 and last 3 characters (e.g. ''' or """) from a string */
    protected static String stripQuotes3(String s) {
        return s.substring(3, s.length() - 3) ;
    }

    /** remove the first n charcacters from the string */
    public static String stripChars(String s, int n) {
        return s.substring(n, s.length()) ;
    }

}
