package minispark.distributed.objectstore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageNode {
    private final Path basePath;
    private final Map<Path, Lock> pathLocks = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(LocalStorageNode.class);

    public LocalStorageNode(String basePath) {
        this(Paths.get(basePath));
    }

    public LocalStorageNode(Path basePath) {
        this.basePath = basePath;
        createDirectoryIfNotExists(basePath);
    }

    public void putObject(String key, byte[] data) throws IOException {
        putObjectWithLock(key, data, true);
    }

    public void putObjectWithLock(String key, byte[] data, boolean overwrite) throws IOException {
        Path filePath = basePath.resolve(key);
        Lock lock = pathLocks.computeIfAbsent(filePath, k -> new ReentrantLock());
        lock.lock();
        try {
            if (!overwrite && Files.exists(filePath)) {
                throw new IOException("File already exists: " + filePath);
            }
            Files.createDirectories(filePath.getParent());
            Path tempFile = Files.createTempFile(filePath.getParent(), "temp_", null);
            try {
                Files.write(tempFile, data);
                Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                Files.deleteIfExists(tempFile);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    public byte[] getObject(String key) throws IOException {
        Path path = basePath.resolve(key);
        if (!Files.exists(path)) {
            throw new IOException("Failed to retrieve object: " + key);
        }
        return Files.readAllBytes(path);
    }

    /**
     * Retrieves a specific byte range from an object, implementing S3-style range reads.
     * This is crucial for efficient Parquet footer reading and row group access.
     * 
     * @param key The object key
     * @param startByte Starting byte position (inclusive, 0-based)
     * @param endByte Ending byte position (inclusive), or -1 for end of file
     * @return The requested byte range
     * @throws IOException if the object doesn't exist or range is invalid
     */
    public byte[] getObjectRange(String key, long startByte, long endByte) throws IOException {
        Path path = basePath.resolve(key);
        if (!Files.exists(path)) {
            throw new IOException("Failed to retrieve object: " + key);
        }

        long fileSize = Files.size(path);
        
        // Handle -1 as end of file
        if (endByte == -1) {
            endByte = fileSize - 1;
        }

        // Validate range
        if (startByte < 0 || startByte >= fileSize) {
            throw new IOException("Invalid start byte: " + startByte + " for file size: " + fileSize);
        }
        if (endByte < startByte || endByte >= fileSize) {
            throw new IOException("Invalid end byte: " + endByte + " for file size: " + fileSize);
        }

        long rangeSize = endByte - startByte + 1;
        if (rangeSize > Integer.MAX_VALUE) {
            throw new IOException("Range too large: " + rangeSize + " bytes");
        }

        // Use RandomAccessFile for efficient range reading
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            byte[] buffer = new byte[(int) rangeSize];
            raf.seek(startByte);
            int bytesRead = raf.read(buffer);
            
            if (bytesRead != rangeSize) {
                throw new IOException("Expected to read " + rangeSize + " bytes but read " + bytesRead);
            }
            
            logger.debug("Read range {}-{} ({} bytes) from object {}", 
                startByte, endByte, rangeSize, key);
            
            return buffer;
        }
    }

    /**
     * Gets the size of an object without reading its content.
     * Equivalent to HTTP HEAD request for object metadata.
     * 
     * @param key The object key
     * @return Size of the object in bytes
     * @throws IOException if the object doesn't exist
     */
    public long getObjectSize(String key) throws IOException {
        Path path = basePath.resolve(key);
        if (!Files.exists(path)) {
            throw new IOException("Failed to get size of object: " + key);
        }
        return Files.size(path);
    }

    public void deleteObject(String key) throws IOException {
        Path path = basePath.resolve(key);
        Files.deleteIfExists(path);
    }

    public List<String> listObjects(String prefix) throws IOException {
        Path prefixPath = prefix.isEmpty() ? basePath : basePath.resolve(prefix);
        if (!Files.exists(prefixPath)) {
            Files.createDirectories(prefixPath);
        }
        logger.debug("Listing objects with prefix: {}, prefixPath: {}", prefix, prefixPath);
        try (Stream<Path> paths = Files.walk(basePath, Integer.MAX_VALUE)) {
            List<String> result = paths
                .filter(p -> {
                    boolean isBasePath = p.equals(basePath);
                    boolean isPrefixPath = !prefix.isEmpty() && p.equals(prefixPath);
                    boolean startsWithPrefix = p.toString().startsWith(prefixPath.toString());
                    boolean isFile = Files.isRegularFile(p);
                    boolean matches = !isBasePath && !isPrefixPath && startsWithPrefix && isFile;
                    logger.debug("Path: {}, isBasePath: {}, isPrefixPath: {}, startsWithPrefix: {}, isFile: {}, matches: {}", 
                               p, isBasePath, isPrefixPath, startsWithPrefix, isFile, matches);
                    return matches;
                })
                .map(p -> {
                    String relative = basePath.relativize(p).toString();
                    logger.debug("Relative path: {}", relative);
                    return relative;
                })
                .collect(Collectors.toList());
            logger.debug("Found {} objects: {}", result.size(), result);
            return result;
        }
    }

    public List<FileStatus> listObjectsWithMetadata(String prefix) throws IOException {
        Path prefixPath = basePath.resolve(prefix);
        if (!Files.exists(prefixPath)) {
            Files.createDirectories(prefixPath);
        }
        try (Stream<Path> paths = Files.walk(prefixPath, 1)) {
            return paths
                .filter(p -> !p.equals(prefixPath))
                .map(p -> {
                    try {
                        return new FileStatus(
                            Files.size(p),
                            Files.isDirectory(p),
                            1, // replication factor
                            4096, // block size
                            Files.getLastModifiedTime(p).toMillis(),
                            p.toUri()
                        );
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to get file status", e);
                    }
                })
                .collect(Collectors.toList());
        }
    }

    public List<FileStatus> listFrom(String prefix, boolean recursive) throws IOException {
        Path prefixPath = basePath.resolve(prefix);
        if (!Files.exists(prefixPath)) {
            Files.createDirectories(prefixPath);
        }
        int maxDepth = recursive ? Integer.MAX_VALUE : 1;
        try (Stream<Path> paths = Files.walk(prefixPath, maxDepth)) {
            return paths
                .filter(p -> !p.equals(prefixPath) && Files.isRegularFile(p))
                .map(p -> {
                    try {
                        return new FileStatus(
                            Files.size(p),
                            Files.isDirectory(p),
                            1, // replication factor
                            4096, // block size
                            Files.getLastModifiedTime(p).toMillis(),
                            p.toUri()
                        );
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to get file status", e);
                    }
                })
                .collect(Collectors.toList());
        }
    }

    private void createDirectoryIfNotExists(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory", e);
        }
    }
} 