/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.system;

import org.apache.jena.sys.JenaSystem ;
import org.seaborne.delta.Delta ;
import org.slf4j.Logger ;

/** Delta server-side. */
public class DeltaSystem {

    /** Development support - flag to enable output during
     * initialization. Output to {@code System.err}, not a logger
     * to avoid the risk of recursive initialization.
     */
    public static boolean DEBUG_INIT = false ;
    public static String NAME = "Delta" ;

    private static Class<DeltaSubsystemLifecycle> classAtRuntime = DeltaSubsystemLifecycle.class;

    /** <b>Startup</b>
     * <p>
     * RDF Delta uses both Jena system initialization and it's own initialization.
     * <p>
     * <a href="https://jena.apache.org/documentation/notes/system-initialization.html">Jena system initialization</a>
     * is used for the libraries (RDF Patch, The Delta base system, and the client code)
     * and Delta initialization for the local server; it runs after Jena initialization.
     * <p>
     * Client and library:
     * <ul>
     * <li>Class InitPatch -- rdf-patch -- calls PatchSystem.init();
     * <li>Class InitDelta -- rdf-delta-base -- calls Delta.init(), sets loggers
     * <li>Class InitDeltaClient -- rdf-delta-client -- calls VocabDelta.init(); sets vocabularies, registers assembler
     * <li>Class InitJenaDeltaServerLocal -- rdf-delta-server-local -- Unused, replaced by delta initialization
     * </ul>
     * <p>
     * Server, in module {@code rdf-delta-server-local}:
     * <ul>
     * <li>Class DeltaInitLevel0 -- hardwired in DeltaSystem -- Ensures JenaSystem.init();
     * <li>Class DeltaInitLevel1 -- First initializer
     * <li>Any PatchStoreProviders, using interface DeltaSubsystemLifecycle
     * <li>Class InitDeltaServerLocal -- after providers; call DPS.init();
     * </ul>
     * Debug flags<br/>
     * {@code JenaSystem.DEBUG_INIT}<br/>
     * {@code DeltaSystem.DEBUG_INIT}<br/>
     */

    public static void init() {
        // Trigger by the local server start.
        // The LocalServer static initializer calls "DPS.init()" calls "DeltaSystem.init()".
        JenaSystem.init();
        Initializer<DeltaSubsystemLifecycle> initializer =
            new Initializer<DeltaSubsystemLifecycle>(classAtRuntime, new DeltaInitLevel0(), DEBUG_INIT, NAME);
        initializer.init();
    }

    public static void logLifecycle(String fmt, Object...args) {
        Initializer.logLifecycle(DEBUG_INIT, "Delta", fmt, args);
    }

    /** The level 0 subsystem - inserted without using the Registry load function.
     *  There should be only one such level 0 handler.
     */
    private static class DeltaInitLevel0 implements DeltaSubsystemLifecycle {
        private Logger log = Delta.DELTA_LOG;
        @Override
        public void start() {
            log.debug("Delta initialization (level 0)");
        }

        @Override
        public void stop() {
            log.debug("Delta shutdown (level 0)");
        }

        @Override
        public int level() {
            return 0;
        }
    }
}
