package minispark.objectstore;

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
import java.util.Comparator;
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