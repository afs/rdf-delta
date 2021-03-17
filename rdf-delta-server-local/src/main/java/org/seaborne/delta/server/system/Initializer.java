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

import java.util.Collections ;
import java.util.Comparator ;
import java.util.List ;
import java.util.function.Consumer ;

/** Simple controls for ensuring components are loaded and initialized.
 * <p>
 * All initialization should be concurrent and thread-safe.  In particular,
 * some subsystems need initialization in some sort of order (e.g. ARQ before TDB).
 * <p>
 * This is achieved by "levels": levels less than 100 are considered "Delta system levels"
 * and are reserved.
 * <ul>
 * <li>0 - reserved
 * <li>10 -
 * <li>20 -
 * <li>30 -
 * <li>40 -
 * <li>9999 - other
 * </ul>
 * See also the <a href="http://jena.apache.org/documentation/notes/system-initialization.html">notes on Jena initialization</a>.
 * <p>
 * This jiggery-pokery is because generics are by erasure.
 */
class Initializer<T extends SubsystemLifecycle> {
    private final boolean DEBUG_INIT ;
    private final T level0 ;
    private final Class<T> cls ;
    private final String NAME ;

    // A correct way to manage without synchonized using the double checked locking pattern.
    //   http://en.wikipedia.org/wiki/Double-checked_locking
    //   http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
    static volatile boolean initialized = false ;
    static Object initLock = new Object() ;

    Initializer(Class<T> cls, T level0, boolean DEBUG_INIT, String NAME) {
        this.cls = cls;
        this.level0 = level0;
        this.DEBUG_INIT = DEBUG_INIT;
        this.NAME = NAME;
    }

    /** Initialize.
     * <p>
     * This function is cheap to call when already initialized so can be called to be sure.
     * A commonly used idiom is a static initializer in key classes.
     * <p>
     * By default, initialization happens by using {@code ServiceLoader.load} to find
     * {@link SubsystemLifecycle} objects.
     * See {@link #setSubsystemRegistry} to intercept that choice.
     */

    void init() {
        // Any other thread attempting to initialize as well will
        // first test the volatile outside the lock; if it's
        // not INITIALIZED, the thread will attempt to grab the lock
        // and hence wait, then see initialized as true.

        // But we need to cope with recursive calls of DeltaSystem.init() as well.
        // The same thread will not stop at the lock.
        // Set initialized to true before a recursive call is possible
        // handles this.  The recursive call will see initialized true and
        // and return on the first test.

        // Net effect:
        // After a top level call of DeltaSystem.init() returns, Delta has
        // finished initialization.
        // Recursive calls do not have this property.

        if ( initialized )
            return ;
        synchronized(initLock) {
            if ( initialized )  {
                logLifecycle("init - return");
                return ;
            }
            // Catches recursive calls, same thread.
            initialized = true ;
            logLifecycle("init - start");

            if ( get() == null )
                setSubsystemRegistry(new SubsystemRegistryBasic<T>(cls)) ;

            get().load() ;

            // Debug : what did we find?
            if ( DEBUG_INIT ) {
                logLifecycle("Found:") ;
                get().snapshot().forEach((T mod)->
                logLifecycle("  %-20s [%d]", mod.getClass().getSimpleName(), mod.level())) ;
            }

            get().add(level0) ;

            if ( DEBUG_INIT ) {
                logLifecycle("Initialization sequence:") ;
                forEach( module ->
                    logLifecycle("  %-20s [%d]", module.getClass().getSimpleName(), module.level()) ) ;
            }

            forEach( module -> {
                logLifecycle("Init: %s", module.getClass().getSimpleName());
                module.start() ;
            }) ;
            logLifecycle("init - finish");
        }
    }

    /** Shutdown subsystems */
    public void shutdown() {
        if ( ! initialized ) {
            logLifecycle("shutdown - not initialized");
            return ;
        }
        synchronized(initLock) {
            if ( ! initialized ) {
                logLifecycle("shutdown - return");
                return ;
            }
            logLifecycle("shutdown - start");
            forEachReverse(module -> {
                logLifecycle("Stop: %s", module.getClass().getSimpleName());
                module.stop() ;
            }) ;
            initialized = false ;
            logLifecycle("shutdown - finish");
        }
    }

    private SubsystemRegistry<T> singleton = null;

    /**
     * Set the {@link SubsystemRegistry}.
     * To have any effect, this function
     * must be called before any other Jena code,
     * and especially before calling {@code init()}.
     */
    public void setSubsystemRegistry(SubsystemRegistry<T> thing) {
        singleton = thing;
    }

    /** The current JenaSubsystemRegistry */
    public SubsystemRegistry<T> get() {
        return singleton;
    }

    /**
     * Call an action on each item in the registry. Calls are made sequentially
     * and in increasing level order. The exact order within a level is not
     * specified; it is not registration order.
     *
     * @param action
     */
    void forEach(Consumer<T> action) {
        forEach(action, comparator);
    }

    /**
     * Call an action on each item in the registry but in the reverse
     * enumeration order. Calls are made sequentially and in decreasing level
     * order. The "reverse" is opposite order to {@link #forEach}, which may not
     * be stable within a level. It is not related to registration order.
     *
     * @param action
     */
    void forEachReverse(Consumer<T> action) {
        forEach(action, reverseComparator);
    }

    // Order by level (increasing)
    private Comparator<T> comparator        = (obj1, obj2) -> Integer.compare(obj1.level(), obj2.level()) ;
    // Order by level (decreasing)
    private Comparator<T> reverseComparator = comparator.reversed();

    private synchronized void forEach(Consumer<T> action, Comparator<T> ordering) {
        List<T> x = get().snapshot() ;
        Collections.sort(x, ordering);
        x.forEach(action);
    }

    /** Output a debugging message if DEBUG_INIT is set */
    void logLifecycle(String fmt, Object ...args) {
        logLifecycle(DEBUG_INIT, NAME, fmt, args);
    }

    /** Output a debugging message if DEBUG_INIT is set */
    static void logLifecycle(boolean DEBUG_INIT, String NAME, String fmt, Object ...args) {
        if ( ! DEBUG_INIT )
            return ;
        if ( NAME != null )
            fmt = NAME+" "+fmt;
        System.err.printf(fmt, args) ;
        System.err.println() ;
    }
}