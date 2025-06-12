package minispark.fileformat;

import minispark.objectstore.Client;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * S3-compatible InputFile implementation for Parquet that uses range reads.
 * This enables efficient reading of Parquet files from object stores without
 * downloading the entire file.
 */
public class ObjectStoreInputFile implements InputFile {
    private static final Logger logger = LoggerFactory.getLogger(ObjectStoreInputFile.class);
    
    private final Client objectStoreClient;
    private final String key;
    private final long fileSize;

    public ObjectStoreInputFile(Client objectStoreClient, String key) throws IOException {
        this.objectStoreClient = objectStoreClient;
        this.key = key;
        
        // Get file size using HEAD-like request
        try {
            this.fileSize = objectStoreClient.getObjectSize(key).get();
            logger.debug("ObjectStoreInputFile for key {} has size {} bytes", key, fileSize);
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to get size for object: " + key, e);
        }
    }

    @Override
    public long getLength() throws IOException {
        return fileSize;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new ObjectStoreSeekableInputStream();
    }

    /**
     * SeekableInputStream implementation that uses range reads for efficient access.
     * This is the key to making Parquet work efficiently with object stores.
     */
    private class ObjectStoreSeekableInputStream extends SeekableInputStream {
        private long position = 0;
        private boolean closed = false;

        @Override
        public void seek(long newPos) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (newPos < 0 || newPos > fileSize) {
                throw new IOException("Invalid seek position: " + newPos + ", file size: " + fileSize);
            }
            position = newPos;
            logger.debug("Seeked to position {} in {}", position, key);
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public int read() throws IOException {
            byte[] buffer = new byte[1];
            int bytesRead = read(buffer, 0, 1);
            return bytesRead == -1 ? -1 : (buffer[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (position >= fileSize) {
                return -1; // EOF
            }

            // Calculate actual read length (don't read past EOF)
            long remainingBytes = fileSize - position;
            int actualLen = (int) Math.min(len, remainingBytes);
            
            if (actualLen <= 0) {
                return -1;
            }

            // Use range read to get the data
            long endByte = position + actualLen - 1;
            logger.debug("Reading range {}-{} ({} bytes) from {} at position {}", 
                position, endByte, actualLen, key, position);

            try {
                byte[] rangeData = objectStoreClient.getObjectRange(key, position, endByte).get();
                
                // Copy to the provided buffer
                System.arraycopy(rangeData, 0, b, off, rangeData.length);
                position += rangeData.length;
                
                logger.debug("Successfully read {} bytes from {}, new position: {}", 
                    rangeData.length, key, position);
                
                return rangeData.length;
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to read range from object: " + key, e);
            }
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (!buf.hasRemaining()) {
                return 0;
            }
            
            byte[] temp = new byte[buf.remaining()];
            int bytesRead = read(temp, 0, temp.length);
            
            if (bytesRead > 0) {
                buf.put(temp, 0, bytesRead);
            }
            
            return bytesRead;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            int totalRead = 0;
            while (totalRead < len) {
                int bytesRead = read(bytes, start + totalRead, len - totalRead);
                if (bytesRead == -1) {
                    throw new IOException("Reached EOF before reading required bytes");
                }
                totalRead += bytesRead;
            }
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int bytesRead = read(buf);
                if (bytesRead == -1) {
                    throw new IOException("Reached EOF before filling buffer");
                }
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            logger.debug("Closed stream for {}", key);
        }

        @Override
        public boolean markSupported() {
            return false; // We don't support mark/reset
        }
    }

    @Override
    public String toString() {
        return String.format("ObjectStoreInputFile{key='%s', size=%d}", key, fileSize);
    }
} 