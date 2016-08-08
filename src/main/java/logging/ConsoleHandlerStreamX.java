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

package logging;

import java.io.OutputStream ;
import java.nio.charset.StandardCharsets ;
import java.util.logging.* ;

import org.apache.jena.atlas.logging.java.TextFormatter ;

/** Console handler that modifies {@link java.util.logging.ConsoleHandler}.
 * Supports the configuration parameters of {@link ConsoleHandler} -- {@code .level},
 * {@code .filter}, {@code .formatter} and {@code .encoding}.
 * <p>
 * Defaults:
 * <ul>
 * <li>Stdout, rather than stderr, by default.</li>
 * <li>{@link TextFormatter} rather than {@link java.util.logging.SimpleFormatter}</li>
 * <li>UTF-8, rather than platform charset</li>
 * </ul>
 */
public class ConsoleHandlerStreamX extends StreamHandler {
    // We can't use ConsoleHandler.  
    // setOutputStream() closes the previous stream but that is System.err
    // System.err is then closed in the app!
    // We need to chose the output in the constructor and ConsoleHandler does not allow that.
    
    public ConsoleHandlerStreamX() {
        this(System.out) ;
    }
    
    public ConsoleHandlerStreamX(OutputStream outputStream) {
        super(outputStream, new TextFormatter()) ;
        
        LogManager manager = LogManager.getLogManager();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader() ;
        String cname = getClass().getName();
        
        // -- Level
        Level level = Level.INFO ;
        String pLevel = getProperty(manager, cname, "level") ;
        if ( pLevel != null )
            level = Level.parse(pLevel) ;
        setLevel(level);
        
        // -- Formatter
        // The default is TextFormatter above
        // (we had to pass a Formatter of some kind to super(,)).
        String pFormatter = getProperty(manager, cname, "formatter") ;
        if ( pFormatter != null ) {
            try {
                Class<?> cls = classLoader.loadClass(pFormatter);
                setFormatter((Formatter) cls.newInstance());
            } catch (Exception ex) {
                System.err.println("Problems setting the logging formatter") ;
                ex.printStackTrace(System.err);
            }
        }
        
        // -- Filter
        String pFilter = getProperty(manager, cname, "filter") ;
        if ( pFilter != null ) {
            try {
                Class<?> cls = classLoader.loadClass(pFilter);
                setFilter((Filter) cls.newInstance());
            } catch (Exception ex) {
                System.err.println("Problems setting the logging filter") ;
                ex.printStackTrace(System.err);
            }
        }
        
        // -- Encoding : Default UTF-8
        String pEncoding = getProperty(manager, cname, "encoding") ;
        if ( pEncoding == null )
            pEncoding = StandardCharsets.UTF_8.name() ;
        try { setEncoding(pEncoding) ; }
        catch (Exception e) { 
            // That should work for UTF-8 as it is a required charset. 
            System.err.print("Failed to set encoding: "+e.getMessage()) ;
        }
    }
    
    private String getProperty(LogManager manager, String cname, String pname) {
        return manager.getProperty(cname+"."+pname);
    }
    
    @Override
    public void publish(LogRecord record) {
        super.publish(record);
        flush();
    }
}
