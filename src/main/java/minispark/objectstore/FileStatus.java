package minispark.objectstore;

import java.net.URI;

public class FileStatus {
    private final long length;
    private final boolean isDirectory;
    private final int replication;
    private final long blockSize;
    private final long modificationTime;
    private final URI uri;

    public FileStatus(long length, boolean isDirectory, int replication, long blockSize, 
                     long modificationTime, URI uri) {
        this.length = length;
        this.isDirectory = isDirectory;
        this.replication = replication;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.uri = uri;
    }

    public long getLength() {
        return length;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public int getReplication() {
        return replication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public URI getUri() {
        return uri;
    }
} 