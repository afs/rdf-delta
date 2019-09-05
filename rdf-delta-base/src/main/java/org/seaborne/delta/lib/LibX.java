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

package org.seaborne.delta.lib;

import java.io.ByteArrayInputStream ;
import java.io.IOException;
import java.io.InputStream ;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.tdb.base.file.Location;

public class LibX {

    /** Convert an exception into a {@link RuntimeException} */
    public static RuntimeException adapt(Exception exception) {
        if ( exception instanceof RuntimeException )
            return (RuntimeException)exception;
        // Wrap: the original is in the "caused by" and there is no stack trace for this method.
        return new RuntimeException(exception.getMessage(), exception) {
            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }
        };
    }

    /** Copy an array of bytes.*/
    public static byte[] copy(byte[] bytes) {
        if ( bytes == null )
            return null ;
        return Arrays.copyOf(bytes, bytes.length) ;
    }

    /** Copy the contents of an {@link InputStream} so it can be closed. */
    public static InputStream copy(InputStream inputStream) {
        if ( inputStream == null )
            return null ;
        byte[] b = IO.readWholeFile(inputStream) ;
        InputStream x = new ByteArrayInputStream(b) ;
        return x ;
    }

    /**
     * Resolve a Location and file path: Location.getPath only handles file names withing
     * the location, not paths.
     */
    public static String resolve(Location location, String pathStr) {
        Path path = Paths.get(pathStr);
        if ( path.getNameCount() == 0 )
            return location.getDirectoryPath();
        else if ( path.getNameCount() == 1 )
            return location.getPath(pathStr);
        Path locationPath = IOX.asPath(location);
        return locationPath.resolve(pathStr).toAbsolutePath().toString();
    }

    public static boolean exactNumNull(int N, Object... objs) {
        return N == countNull(objs);
    }

    public static int countNull(Object... objs) {
        int x = 0;
        for ( Object obj : objs ) {
            if ( obj == null )
                x++;
        }
        return x ;
    }

    public static int countNonNulls(Object ... objects) {
        int x = 0;
        for ( Object obj : objects ) {
            if ( obj != null )
                x++;
        }
        return x;
    }

    public static boolean allNonNull(Object ... objects) {
        return countNonNulls(objects) == objects.length;
    }

    public static boolean exactlyOneNotNull(Object ... objects) {
        return countNonNulls(objects) == 1;
    }

    public static RuntimeException wrap(Throwable th) { return new WrappedException(th); }
    private static class WrappedException extends RuntimeException {
        public WrappedException(Throwable th) { super(th); }
        @Override public synchronized Throwable fillInStackTrace() { return this; }
    }

    /** Test whether a port on the local machine is in user. */
    public static boolean isPortInUser(int port) {
        try ( ServerSocket socket = new ServerSocket(port) ) {
            return false;
        } catch (IOException ex) { return true; }
    }

    @SafeVarargs
    public static <X> X firstNonNull(X ... objs) {
        for ( X x : objs )
            if ( x != null )
                return x;
        return null;
    }
}
