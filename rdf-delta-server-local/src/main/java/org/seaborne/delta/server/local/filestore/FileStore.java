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

package org.seaborne.delta.server.local.filestore;

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.nio.file.DirectoryStream ;
import java.nio.file.Files ;
import java.nio.file.NoSuchFileException ;
import java.nio.file.Path ;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong ;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DeltaConst ;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.DeltaNotFoundException ;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.IOX.IOConsumer;
import org.seaborne.patch.PatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code FileStore} is a collection of files where the file names have a common pattern
 * and the files are stored in the same location.
 * <p>
 * The set of files is from a basename, with new files being "BASE-0001", "BASE-0002",
 * etc. The basename must match the pattern <tt>[a-zA-Z]([_a-zA-Z0-9])*</tt> and not end with the "-".
 * <p>
 * In addition, it is possible to allocate a fresh filename (no file with that name existed before) and an
 * associated temporary file. This supports atomically writing new data; see {@link #writeNewFile}.
 * <p>
 * The basename "tmp" is reserved.
 * <p>
 * Once written, files are not changed.
 */
public class FileStore {
    private static Logger       LOG = LoggerFactory.getLogger(FileStore.class);

    // Key'ed by directory and log name.
    // One FileStore per location on disk.
    private static Map<Path, FileStore> areas = new ConcurrentHashMap<>();

    private static final String tmpBasename = "tmp";
    private static final Pattern basenamePattern = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]*");
    private static final String SEP = "-";

    private static final int BUFSIZE = 128*1024;

    // Setting for "no files": start at one less than the first allocated number.
    private long minIndex;
    private final List<Long> indexes;

    // Index - this is the number of the last allocation.
    // It is incremented then a new number taken.
    private final AtomicLong    counter;
    private final Path          directory;
    private final String        basename;

    public static FileStore attach(Path dirname, String basename) {
        Objects.requireNonNull(dirname, "argument 'dirname' is null");
        Objects.requireNonNull(basename, "argument 'basename' is null");
        if ( basename.equals(tmpBasename) )
            throw new IllegalArgumentException("FileStore.attach: basename is equal to the reserved name '"+tmpBasename+"'") ;
        if ( basename.endsWith(SEP) )
            throw new IllegalArgumentException("FileStore.attach: basename ends with the separator: '"+SEP+"'");
        if ( ! basenamePattern.matcher(basename).matches() )
            throw new IllegalArgumentException("FileStore.attach: basename does not match the regex "+basenamePattern);
        Path dirPath = dirname;
        Path k = key(dirPath, basename);
        // [FILE2] computeIfAbsent.
        if ( areas.containsKey(k) )
            return areas.get(k);
        if ( ! Files.exists(dirPath) || ! Files.isDirectory(dirPath) )
            throw new IllegalArgumentException("FileStore.attach: Path '" + dirPath + "' does not name a directory");

        // Delete any tmp files left lying around.
        List<String> tmpFiles = scanForTmpFiles(dirPath);
        tmpFiles.forEach(FileOps::delete);

        // Find existing files.
        List<Long> indexes = scanForIndex(dirPath, basename);
        long min;
        long max;
        if ( indexes.isEmpty() ) {
            min = DeltaConst.VERSION_INIT;
            // So increment is the next version.
            max = DeltaConst.VERSION_FIRST - 1;
            FmtLog.debug(LOG, "FileStore : index [--,--] %s", dirname);
        } else {
            min = indexes.get(0);
            max = indexes.get(indexes.size()-1);
            FmtLog.debug(LOG, "FileStore : index [%d,%d] %s", min, max, dirname);
        }
        FileStore fs = new FileStore(dirPath, basename, indexes, min, max);
        areas.put(k, fs);
        return fs;
    }

    private static Path key(Path path, String basename) {
        Path p = path.resolve(basename);
        return p.normalize().toAbsolutePath();
    }

    private FileStore(Path directory, String basename, List<Long> indexes, long minIndex, long maxIndex) {
        this.directory = directory;
        this.basename = basename;
        // Record initial setup
        this.minIndex = minIndex;
        // Version management.
        this.indexes = indexes;
        this.counter = new AtomicLong(maxIndex);
        deleteFiles(directory, tmpBasename);
    }

    /**
     * Return the {@code Path} of the area being managed.
     */
    public Path getPath() { return directory ; }

    /**
     * Return the index of the last allocation. Return the integer before the first
     * allocation if there has been no allocation.
     */
    public long getCurrentIndex() {
        long x = counter.get();
        return x;
    }

    private long nextIndex() {
        long x = counter.incrementAndGet();
        indexes.add(x);
        return x;
    }

    /**
     * Return the index of the first allocation. Return the integer before the first
     * allocation if there has been no allocation.
     */
    public long getMinIndex() {
        return minIndex;
    }

    /**
     * Return the indexes as a sequential stream from low to high.
     */
    public Stream<Long> getIndexes() {
        return indexes.stream();
    }

    /**
     * Is the file store empty?
     */
    public boolean isEmpty() {
        return indexes.isEmpty();
    }

    /** Return an {@code InputStream} to data for {@code idx}.
     * The {@code InputStream} is not buffered.
     * The caller is responsible for closing the {@code InputStream}.
     */
    public InputStream open(long idx) {
        Path path = filename(idx);
        try {
            return Files.newInputStream(path);
        } catch (NoSuchFileException ex) {
            throw new DeltaNotFoundException(ex.getMessage());
        } catch (IOException ex) {
            throw IOX.exception(ex);
        }
    }

    /**
     * Return details of the next file slot to use in the file store. The file for
     * this name does not exist.
     * <p>
     * This operation is thread-safe.
     */
    public FileEntry nextFilename() {
        return allocateFilename();
    }

    /**
     * Return the {@link #nextFilename} along with a temporary filename.
     * <p>
     * This operation is thread-safe.
     */
    private FileEntry allocateFilename() {
        // --> IOX
        // TODO Use Files.createTempFile? Or does recovery mean we need more control?
        synchronized(this) {
            for ( ;; ) {
                long idx = nextIndex();
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
                return new FileEntry(idx, fn, tmpFn) ;
            }
        }
    }

    // [FILE2]
    // Better? IOConsumer to take version as well as OutputStream.
    // This assumes the version/idx is safely allocated elsewhere.

    /** Allocate a {@link FileEntry} based on the callers choice of index.
     * This must be greater than the current index.
     */
    public FileEntry allocateFilename(long idx) {
        synchronized(this) {
            // Ensure this is "+1" -- more restrictive than contract ATM.
            long v = counter.get();
            if ( idx != v+1 )
                throw new DeltaException("FileStore.allocateFilename(idx): Not an incremental file version");
            counter.set(idx);
            Path fn = filename(idx);
            if ( Files.exists(fn) ) {
                FmtLog.error(LOG, "Existing file: %s", fn);
                throw new DeltaException("Existing file: "+fn);
            }
            Path tmpFn = filename(directory, tmpBasename, idx);
            if ( Files.exists(tmpFn) ) {
                FmtLog.error(LOG, "Existing tmp file: %s", tmpFn);
                throw new DeltaException("Existing tmp file: "+tmpFn);
            }
            return new FileEntry(idx, fn, tmpFn) ;
        }
    }

    /** Write a fresh file, safely.
     * <p>
     * This operation writes to a temporary file on the same filesystem, then moves it to
     * the new location. Therefore it is atomic.
     * @param action The code to write the contents.
     * @returns Path to the new file.
     */
    public FileEntry writeNewFile(IOConsumer<OutputStream> action) {
        FileEntry file = allocateFilename();
        file.write(action);
        completeWrite(file);
        return file;
    }

    public void completeWrite(FileEntry entry) {
        if ( minIndex == DeltaConst.VERSION_INIT || minIndex == DeltaConst.VERSION_UNSET )
            minIndex = entry.version;
    }

    /** Release this {@code FileStore} - do not use again. */
    public void release() {
        // Overlapping outstanding operations can continue.
        Path k = key(directory, basename);
        FileStore old = areas.remove(k);
        if ( old == null )
            FmtLog.warn(LOG, "Releasing non-existent FileStore: (%s, %s)", directory, basename);
    }

    /** Stop managing files */
    public static void resetTracked() {
        areas.clear();
    }

    /**
     * Basename of a file for the index. This does not mean the file exists.
     */
    public String basename(long idx) {
        return basename(basename, idx);
    }

    @Override
    public String toString() {
        return "FileStore["+directory+", "+basename+"]";
    }

    private static String basename(String base, long idx) {
        if ( idx < 0 )
            throw new IllegalArgumentException("idx = " + idx);
        return String.format("%s%s%04d", base, SEP, idx);
    }

    private static int extractIndex(String name, String namebase) {
        int i = namebase.length()+SEP.length();
        String numStr = name.substring(i);
        int num = Integer.parseInt(numStr);
        return num;
    }

    /**
     * Return an absolute filename to a file in the fileset with index {@code idx}. There
     * is no guarantee that the file exists.
     * <p>
     * To get an unused file for adding new data, to the FileStore, use
     * {@link #nextFilename}. Use {@link #allocateFilename()} to get a pair of file name
     * and temporary file in the same FileStore.
     */
    public Path filename(long idx) {
        return filename(directory, basename, idx);
    }

    private static Path filename(Path dir, String basename, long idx) {
        String fn = basename(basename, idx);
        return dir.resolve(fn);
    }

    private static List<String> scanForTmpFiles(Path directory) {
        List<String> tmpFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*."+tmpBasename)) {
            for ( Path f : stream ) {
                if ( Files.isRegularFile(f) )
                    tmpFiles.add(f.toAbsolutePath().toString());
            }
        } catch (IOException ex) {
            FmtLog.warn(LOG, "Can't inspect directory: %s", directory);
            throw new PatchException(ex);
        }
        return tmpFiles;
    }

    public List<Long> scanIndex() {
       return scanForIndex(directory, basename);
    }

    /** Find the indexes of files in this FileStore. Return sorted, low to high. */
    private static List<Long> scanForIndex(Path directory, String namebase) {
        List<Long> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, namebase + "*")) {
            for ( Path f : stream ) {
                long num = extractIndex(f.getFileName().toString(), namebase);
                if ( num == -1 ) {
                    FmtLog.warn(LOG, "Can't parse filename: %s", f.toString());
                    continue;
                }
                indexes.add(num);
            }
        } catch (IOException ex) {
            FmtLog.warn(LOG, "Can't inspect directory: (%s, %s)", directory, namebase);
            throw new PatchException(ex);
        }

        indexes.sort(Long::compareTo);
        return indexes;
    }

    /** Find the indexes of files in this FileStore. Return sorted, low to high. */
    private static boolean deleteTmpFiles(Path directory) {
        boolean found = false;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, tmpBasename + "*")) {
            for ( Path f : stream ) {
                found = true;
                Files.delete(f);
                if ( Files.exists(f) )
                    FmtLog.error(LOG, "Failed to delete tmp file: %s", f);
            }
        } catch (IOException ex) {
            FmtLog.warn(LOG, "Can't check directory for tmp files: %s", directory);
            throw new PatchException(ex);
        }
        return found;
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
}
