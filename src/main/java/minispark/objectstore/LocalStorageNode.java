package minispark.objectstore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class LocalStorageNode {
    private final Path storageDirectory;

    public LocalStorageNode(String directory) throws IOException {
        this.storageDirectory = Paths.get(directory);
        Files.createDirectories(storageDirectory);
    }

    public void putObject(String key, byte[] data) {
        try {
            Path objectPath = storageDirectory.resolve(key);
            Files.write(objectPath, data);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to store object: " + key, e);
        }
    }

    public byte[] getObject(String key) {
        try {
            Path objectPath = storageDirectory.resolve(key);
            return Files.readAllBytes(objectPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to retrieve object: " + key, e);
        }
    }

    public void deleteObject(String key) {
        try {
            Path objectPath = storageDirectory.resolve(key);
            Files.deleteIfExists(objectPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete object: " + key, e);
        }
    }

    public List<String> listObjects() {
        try {
            return Files.list(storageDirectory)
                .map(path -> path.getFileName().toString())
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list objects", e);
        }
    }
} 