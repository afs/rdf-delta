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

package org.seaborne.delta.server;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.patch.PatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code FileStore} is a collection of files where the file names have a common pattern.
 * <p>
 * The set of files is from a basename, with new files being "BASE-0001", "BASE-0002",
 * etc.
 * <p>
 * In addition,  it is possible to allocate a fresh filename (no file with that name existed before) and an
 * associated temporary file. This supports atomically writing new data; see {@link #writeNewFile}.
 * <p>
 * The basename "tmp" is reserved.
 */
public class FileStore {
    private static Logger               LOG   = LoggerFactory.getLogger(FileStore.class);
    // Key'ed by directory and name name.
    private static Map<Path, FileStore> areas = new ConcurrentHashMap<>();

    public static FileStore attach(String dirname, String basename) {
        Objects.requireNonNull(dirname, "argument 'dirname' is null");
        Objects.requireNonNull(basename, "argument 'basename' is null");
        if ( basename.equals(tmpBasename) )
            throw new IllegalArgumentException("basename is equal to the reserved name '"+tmpBasename+"'") ;
        Path path = string2path(dirname);
        Path k = key(path, basename);
        if ( areas.containsKey(k) )
            return areas.get(k);
        int idx = scanForIndex(path, basename);
        if ( idx == 0 )
        if ( idx == -1 )
            throw new IllegalArgumentException("Path '" + path + "' does not name a directory");
        FileStore fs = new FileStore(path, basename, idx);
        areas.put(k, fs);
        return fs;
    }
    
    private static Path string2path(String pathname) {
        try {
            return Paths.get(pathname).normalize().toRealPath();
        } catch (IOException ex) { IO.exception(ex); return null; }
    }

    private static Path key(Path path, String basename) {
        Path p = path.resolve(basename);
        return p.normalize().toAbsolutePath();
    }

    private static final String tmpBasename = "tmp";
    private static final int BUFSIZE = 128*1024;
    // Setting for "no files" which is one less than the first allocated number. 
    private static final int INITIAL = 0;
    // Index - this is the number of the last allocation.
    // It is incremented then a new number taken.
    private final AtomicInteger counter;
    private final Path          directory;
    private final String        basename;

    private FileStore(Path directory, String basename, int initialIndex) {
        this.directory = directory;
        this.basename = basename;
        this.counter = new AtomicInteger(initialIndex);
        deleteFiles(directory, tmpBasename);
    }

    /**
     * Return the index of the last allocation. Return the integer before the first
     * allocation if there has been no allocation.
     */
    public int getCurrentIndex() {
        return counter.get();
    }

    /**
     * Return an absolute filename to the next file to use in the file store. The file for
     * this name does not exist.
     * <p>
     * This operation is thread-safe.
     * <p>
     * Use {@link #allocateFilename} to get a filename and a related tmp file.
     */
    public Path nextFilename() {
        return allocateFilename().getLeft();
    }

    /**
     * Return the {@link #nextFilename} along with a temporary filename.
     * <p>
     * This operation is thread-safe.
     */
    public Pair<Path, Path> allocateFilename() {
        synchronized(this) { 
            for ( ;; ) {
                int idx = nextIndex();
                Path fn = filename(idx);
                if ( Files.exists(fn) ) {
                    FmtLog.warn(LOG, "Skipping existing file: %s", fn);
                    continue;
                }
                Path tmpFn = filename(directory, tmpBasename, idx);
                if ( Files.exists(tmpFn) ) {
                    FmtLog.warn(LOG, "Skipping existing tmp file: %s", tmpFn);
                    continue;
                }
                return Pair.create(fn, tmpFn);
            }
        }
    }
    
    /** Write a fresh file, safely.
     * <p>
     * This operation writes to a temporary file on the same filesystem, then moves it to
     * the new location. Therefore it is atomic.
     * @params Consumer The code to write the contents.
     * @returns Path to the new file.
     */
    public Path writeNewFile(Consumer<OutputStream> action) {
        Pair<Path, Path> p = allocateFilename(); // (file,tmp)
        Path file = p.getLeft();
        Path tmp = p.getRight();
        
        try ( OutputStream out = new BufferedOutputStream(Files.newOutputStream(tmp))) {
            // Write contents.
            action.accept(out);
        } catch(IOException ex) { IO.exception(ex); }
        // tmp closed.
        // Move - same file system means this is atomic.
        move(tmp,file) ;
        return file;
    }
    
    /** Stop managing files */
    public static void shutdown() {
        areas.clear();
    }

    
    //Move a complete file into place
    private static void move(Path src, Path dst) {
        try { Files.move(src, dst) ; }
        catch (IOException ex) {
            LOG.warn(String.format("IOException moving %s to %s", src, dst) , ex);
            IO.exception(ex);
        }
    }

    private int nextIndex() {
        return counter.incrementAndGet();
    }

    /**
     * Basename of a file for the index. This does not mean the file exists.
     */
    public String basename(int idx) {
        return basename(basename, idx);
    }

    private static String basename(String base, int idx) {
        if ( idx < 0 )
            throw new IllegalArgumentException("idx = " + idx);
        return String.format(base + "-%04d", idx);
    }

    /**
     * Return an absolute filename to a file in the fileset with index {@code idx}. There
     * is no guarantee that the file exists.
     * <p>
     * To get an unused file for adding new data, to the FileStore, use
     * {@link #nextFilename}. Use {@link #allocateFilename()} to get a pait of file name
     * and temporary file in the same FileStore.
     */
    public Path filename(int idx) {
        return filename(directory, basename, idx);
    }

    private static Path filename(Path dir, String basename, int idx) {
        String fn = basename(basename, idx);
        return dir.resolve(fn);
    }

    /** Find the highest index in a directory of files */
    private static int scanForIndex(Path directory, String namebase) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, namebase + "*")) {
            int max = INITIAL;
            for ( Path f : stream ) {
                int num = extractIndex(f.getFileName().toString(), namebase);
                if ( num == -1 )
                    FmtLog.warn(LOG, "Can't parse filename: %s", f.toString());
                else
                    max = Math.max(max, num);
            }
            return max;
        } catch (IOException ex) {
            FmtLog.warn(LOG, "Can't inspect directory: (%s, %s)", directory, namebase);
            throw new PatchException();
        }
    }

    /**
     * Remove all files matching the temporary file template 
     */
    private static void deleteFiles(Path directory, String template) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, template+"*")) {
            for ( Path f : stream ) {
                try {
                    Files.delete(f);
                } catch (IOException ex) {
                    FmtLog.warn(LOG, "Can't delete file: %s", f) ;
                }
            }
        } catch (IOException ex) {
            FmtLog.warn(LOG, "Can't inspect directory for tmp files: %s");
            throw new PatchException();
        }
    }

    private static int extractIndex(String name, String namebase) {
        Pattern pattern = Pattern.compile(namebase + "-([0-9]*)");
        Matcher m = pattern.matcher(name);
        if ( !m.matches() )
            return -1;
        String numStr = m.group(1);
        int num = Integer.parseInt(numStr);
        return num;
    }
}
