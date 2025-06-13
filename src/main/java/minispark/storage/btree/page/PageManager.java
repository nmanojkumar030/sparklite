package minispark.storage.btree.page;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages reading and writing pages to disk.
 * Includes logging for educational purposes to show page access patterns.
 */
public class PageManager implements AutoCloseable {
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final int pageSize;
    private final AtomicLong nextPageId;
    
    // Counters for tracking page access statistics
    private final AtomicLong pageReadsCount = new AtomicLong(0);
    private final AtomicLong pageWritesCount = new AtomicLong(0);
    
    /**
     * Creates a new page manager.
     *
     * @param filePath Path to the database file
     * @param pageSize Size of each page in bytes
     * @throws IOException If an I/O error occurs
     */
    public PageManager(Path filePath, int pageSize) throws IOException {
        this.file = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = file.getChannel();
        this.pageSize = pageSize;
        
        // Initialize next page ID from file size
        long fileSize = file.length();
        this.nextPageId = new AtomicLong(fileSize / pageSize);
        
        System.out.println("📁 PageManager initialized for file: " + filePath.getFileName());
        System.out.println("   Page size: " + pageSize + " bytes");
        System.out.println("   Initial file size: " + fileSize + " bytes");
        System.out.println("   Initial page count: " + (fileSize / pageSize));
        System.out.println();
    }
    
    /**
     * Gets the page size in bytes.
     *
     * @return The page size
     */
    public int getPageSize() {
        return pageSize;
    }
    
    /**
     * Gets the file size in bytes.
     *
     * @return The file size
     * @throws IOException If an I/O error occurs
     */
    public long getFileSize() throws IOException {
        return file.length();
    }
    
    /**
     * Allocates a new page.
     *
     * @return The ID of the allocated page
     * @throws IOException If an I/O error occurs
     */
    public long allocatePage() throws IOException {
        long pageId = nextPageId.getAndIncrement();
        
        // Extend file if necessary
        long requiredSize = (pageId + 1) * pageSize;
        if (file.length() < requiredSize) {
            file.setLength(requiredSize);
        }
        
        // Create and initialize the page
        Page page = new Page(pageSize, pageId);
        writePage(page);
        
        System.out.println("🆕 Allocated new page: " + pageId);
        
        return pageId;
    }
    
    /**
     * Reads a page from disk.
     *
     * @param pageId The page ID
     * @return The page
     * @throws IOException If an I/O error occurs
     */
    public Page readPage(long pageId) throws IOException {
        long readCount = pageReadsCount.incrementAndGet();
        
        System.out.println("📖 PAGE READ #" + readCount + " - Reading page " + pageId + " from disk");
        
        // Check if page exists
        long fileSize = file.length();
        long pageOffset = pageId * pageSize;
        
        if (pageOffset >= fileSize) {
            throw new IOException("Page " + pageId + " does not exist (offset " + pageOffset + " >= file size " + fileSize + ")");
        }
        
        // Read page data
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        channel.position(pageOffset);
        int bytesRead = channel.read(buffer);
        
        if (bytesRead != pageSize) {
            throw new IOException("Failed to read page " + pageId + 
                ": expected " + pageSize + " bytes but got " + bytesRead);
        }
        
        // Create page and set data
        Page page = new Page(pageSize, pageId);
        page.setData(buffer.array());
        
        System.out.println("   ✅ Successfully read page " + pageId + " (" + pageSize + " bytes)");
        
        return page;
    }
    
    /**
     * Writes a page to disk.
     *
     * @param page The page to write
     * @throws IOException If an I/O error occurs
     */
    public void writePage(Page page) throws IOException {
        long writeCount = pageWritesCount.incrementAndGet();
        long pageId = page.getPageId();
        
        System.out.println("💾 PAGE WRITE #" + writeCount + " - Writing page " + pageId + " to disk");
        
        long pageOffset = pageId * pageSize;
        
        // Ensure file is large enough
        long requiredSize = pageOffset + pageSize;
        if (file.length() < requiredSize) {
            file.setLength(requiredSize);
        }
        
        // Write page data
        channel.position(pageOffset);
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        int bytesWritten = channel.write(buffer);
        
        if (bytesWritten != pageSize) {
            throw new IOException("Failed to write page " + pageId + 
                ": expected to write " + pageSize + " bytes but wrote " + bytesWritten);
        }
        
        // Force to disk
        channel.force(false);
        
        System.out.println("   ✅ Successfully wrote page " + pageId + " (" + pageSize + " bytes)");
    }
    
    /**
     * Prints page access statistics.
     */
    public void printAccessStatistics() {
        System.out.println();
        System.out.println("📊 PAGE ACCESS STATISTICS:");
        System.out.println("   Total page reads:  " + pageReadsCount.get());
        System.out.println("   Total page writes: " + pageWritesCount.get());
        System.out.println("   Total page I/O:    " + (pageReadsCount.get() + pageWritesCount.get()));
        System.out.println();
    }
    
    /**
     * Resets page access counters.
     */
    public void resetAccessCounters() {
        pageReadsCount.set(0);
        pageWritesCount.set(0);
        System.out.println("🔄 Page access counters reset");
        System.out.println();
    }
    
    /**
     * Gets the total number of page reads.
     */
    public long getPageReadsCount() {
        return pageReadsCount.get();
    }
    
    /**
     * Gets the total number of page writes.
     */
    public long getPageWritesCount() {
        return pageWritesCount.get();
    }
    
    /**
     * Closes the page manager and releases resources.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        System.out.println("🔒 Closing PageManager");
        printAccessStatistics();
        channel.close();
        file.close();
    }
} 