package minispark.storage.btree;

import minispark.storage.Record;
import minispark.storage.StorageInterface;
import minispark.storage.btree.page.Page;
import minispark.storage.btree.page.PageManager;
import minispark.storage.btree.page.Element;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

/**
 * B+Tree storage implementation.
 * Optimized for OLTP workloads with efficient point queries
 * and range scans. Maintains data in sorted order.
 * 
 * CURRENT STATUS: Basic functionality implemented (5/8 tests passing)
 * 
 * ✅ IMPLEMENTED FEATURES:
 * - Basic write/read operations
 * - Batch operations  
 * - Range scanning (basic cases)
 * - Column filtering
 * - Value serialization/deserialization
 * - Page management and disk I/O
 * - Tree initialization and root page management
 * 
 * TODO: FEATURES TO IMPLEMENT:
 * 
 * 1. TODO: Fix Range Scanning Edge Cases
 *    - Empty range scanning (when start key > all existing keys)
 *    - Proper navigation to correct starting leaf page
 *    - Handle cases where range spans multiple pages
 * 
 * 2. TODO: Implement Overflow Pages for Large Values
 *    - createOverflowPages() method implementation
 *    - readFromOverflowPages() method implementation  
 *    - freeOverflowPages() method implementation
 *    - Support for values larger than page size
 * 
 * 3. TODO: Add Thread Safety and Concurrent Access
 *    - Add synchronization mechanisms (ReadWriteLock)
 *    - Implement page-level locking
 *    - Handle concurrent reads and writes safely
 *    - Add proper transaction isolation
 * 
 * 4. TODO: Implement Delete Operations
 *    - delete() method implementation
 *    - Handle key removal from leaf pages
 *    - Implement page merging when underutilized
 *    - Update parent nodes after deletions
 * 
 * 5. TODO: Implement B+Tree Splitting and Merging
 *    - Page splitting when pages become full
 *    - Page merging when pages become underutilized
 *    - Parent node updates during splits/merges
 *    - Maintain B+Tree invariants
 * 
 * 6. TODO: Performance Optimizations
 *    - Page caching mechanism
 *    - Bulk loading for better initial tree structure
 *    - Compression for keys and values
 *    - Statistics collection for query optimization
 * 
 * 7. TODO: Robustness and Error Handling
 *    - Better error recovery mechanisms
 *    - Corruption detection and repair
 *    - Proper resource cleanup on failures
 *    - Comprehensive logging and monitoring
 */
public class BTree implements StorageInterface {
    // Page size in bytes (default 4KB)
    private static final int DEFAULT_PAGE_SIZE = 4096;
    
    // B+Tree degree (max children per node)
    private final int degree;
    
    // Page manager for disk storage
    private final PageManager pageManager;
    
    // Root page ID
    private long rootPageId;
    
    // Serializer/deserializer for values
    private final ValueSerializer valueSerializer;
    
    /**
     * Creates a new B+Tree with a default configuration
     *
     * @param filePath Path to the database file
     * @throws IOException If an I/O error occurs
     */
    public BTree(Path filePath) throws IOException {
        this(filePath, DEFAULT_PAGE_SIZE);
    }
    
    /**
     * Creates a new B+Tree with a custom page size
     *
     * @param filePath Path to the database file
     * @param pageSize Size of each page in bytes
     * @throws IOException If an I/O error occurs
     */
    public BTree(Path filePath, int pageSize) throws IOException {
        this.pageManager = new PageManager(filePath, pageSize);
        this.valueSerializer = new ValueSerializer();
        
        // Calculate degree based on page size
        this.degree = (pageSize - Page.PAGE_HEADER_SIZE) / (Page.ELEM_SIZE * 2);
        
        // Initialize the tree if it doesn't exist
        try {
            initializeTree();
        } catch (Exception e) {
            throw new IOException("Failed to initialize B+Tree", e);
        }
    }
    
    /**
     * Initializes the B+Tree structure if it doesn't exist
     */
    private void initializeTree() throws IOException {
        try {
            // Check if we have any pages in the file
            if (pageManager.getFileSize() == 0) {
                createRootPage();
            } else {
                // File exists, try to read root page (page 0)
                rootPageId = 0;
                Page rootPage = pageManager.readPage(rootPageId);
                int flags = rootPage.flags();
                if (flags == 0) {
                    // Root page is uninitialized, initialize it
                    rootPage.setFlags(Page.FLAG_LEAF);
                    pageManager.writePage(rootPage);
                }
            }
        } catch (IOException e) {
            // Root doesn't exist or is invalid, create it
            createRootPage();
        }
    }
    
    /**
     * Creates a new root page as a leaf node
     */
    private void createRootPage() throws IOException {
        // Allocate root page (should be page 0)
        rootPageId = pageManager.allocatePage();
        Page rootPage = pageManager.readPage(rootPageId);
        rootPage.setFlags(Page.FLAG_LEAF); // Start with a leaf node as root
        pageManager.writePage(rootPage);
    }
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        System.out.println("🔍 BTree.write() - Writing key: " + new String(key));
        
        // Serialize the value to bytes
        byte[] valueBytes = valueSerializer.serialize(value);
        
        // Insert into the tree, handling splits as needed
        SplitResult splitResult = insertIntoTree(rootPageId, key, valueBytes, true);
        
        // If root split, create new root
        if (splitResult != null) {
            createNewRoot(splitResult);
        }
        
        System.out.println("   ✅ Successfully wrote key: " + new String(key));
        System.out.println();
    }
    
    /**
     * Result of a page split operation
     */
    private static class SplitResult {
        final long leftPageId;
        final long rightPageId;
        final byte[] separatorKey;
        
        SplitResult(long leftPageId, long rightPageId, byte[] separatorKey) {
            this.leftPageId = leftPageId;
            this.rightPageId = rightPageId;
            this.separatorKey = separatorKey;
        }
    }
    
    /**
     * Inserts a key-value pair into the tree, handling splits recursively
     */
    private SplitResult insertIntoTree(long pageId, byte[] key, byte[] value, boolean isLeaf) throws IOException {
        Page page = pageManager.readPage(pageId);
        
        if (page.isLeaf()) {
            return insertIntoLeafPage(page, key, value);
        } else {
            return insertIntoBranchPage(page, key, value);
        }
    }
    
    /**
     * Inserts into a leaf page, splitting if necessary
     */
    private SplitResult insertIntoLeafPage(Page leafPage, byte[] key, byte[] value) throws IOException {
        // Try to insert directly
        if (leafPage.insert(key, value)) {
            // Success - no split needed
            pageManager.writePage(leafPage);
            return null;
        }
        
        // Page is full - need to split
        System.out.println("   📄 Leaf page " + leafPage.getPageId() + " is full, splitting...");
        return splitLeafPage(leafPage, key, value);
    }
    
    /**
     * Inserts into a branch page, handling child splits
     */
    private SplitResult insertIntoBranchPage(Page branchPage, byte[] key, byte[] value) throws IOException {
        // Find the child page to insert into
        int childIndex = findChildIndex(branchPage, key);
        Element childElement = branchPage.element(Math.min(childIndex, branchPage.count() - 1));
        
        ByteBuffer buffer = ByteBuffer.wrap(childElement.value());
        long childPageId = buffer.getLong();
        
        // Recursively insert into child
        SplitResult childSplit = insertIntoTree(childPageId, key, value, false);
        
        if (childSplit == null) {
            // No split occurred in child
            return null;
        }
        
        // Child split - need to insert separator key into this branch page
        byte[] separatorKey = childSplit.separatorKey;
        byte[] rightPageIdBytes = ByteBuffer.allocate(8).putLong(childSplit.rightPageId).array();
        
        if (branchPage.insert(separatorKey, rightPageIdBytes)) {
            // Successfully inserted separator - no split needed
            pageManager.writePage(branchPage);
            return null;
        }
        
        // Branch page is also full - need to split
        System.out.println("   🌳 Branch page " + branchPage.getPageId() + " is full, splitting...");
        return splitBranchPage(branchPage, separatorKey, rightPageIdBytes);
    }
    
    /**
     * Splits a full leaf page
     */
    private SplitResult splitLeafPage(Page leftPage, byte[] newKey, byte[] newValue) throws IOException {
        // Collect all elements including the new one
        List<Element> allElements = new ArrayList<>();
        
        // Add existing elements
        for (int i = 0; i < leftPage.count(); i++) {
            allElements.add(leftPage.element(i));
        }
        
        // Add new element in sorted position
        Element newElement = new Element(newKey, newValue, false);
        boolean inserted = false;
        for (int i = 0; i < allElements.size(); i++) {
            if (compareKeys(newKey, allElements.get(i).key()) < 0) {
                allElements.add(i, newElement);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            allElements.add(newElement);
        }
        
        // Create new right page
        long rightPageId = pageManager.allocatePage();
        Page rightPage = pageManager.readPage(rightPageId);
        rightPage.setFlags(Page.FLAG_LEAF);
        
        // Split elements between left and right pages
        int midPoint = allElements.size() / 2;
        
        // Clear left page and add first half of elements
        leftPage.setCount(0);
        for (int i = 0; i < midPoint; i++) {
            Element elem = allElements.get(i);
            leftPage.insert(elem.key(), elem.value(), elem.hasOverflow());
        }
        
        // Add second half to right page
        for (int i = midPoint; i < allElements.size(); i++) {
            Element elem = allElements.get(i);
            rightPage.insert(elem.key(), elem.value(), elem.hasOverflow());
        }
        
        // Link leaf pages
        rightPage.setNextPageId(leftPage.nextPageId());
        leftPage.setNextPageId(rightPageId);
        
        // Write both pages
        pageManager.writePage(leftPage);
        pageManager.writePage(rightPage);
        
        // Return split result with first key of right page as separator
        byte[] separatorKey = allElements.get(midPoint).key();
        System.out.println("   ✅ Split leaf page " + leftPage.getPageId() + " -> " + 
                          leftPage.getPageId() + " + " + rightPageId + 
                          " (separator: " + new String(separatorKey) + ")");
        
        return new SplitResult(leftPage.getPageId(), rightPageId, separatorKey);
    }
    
    /**
     * Splits a full branch page
     */
    private SplitResult splitBranchPage(Page leftPage, byte[] newKey, byte[] newValue) throws IOException {
        // Collect all elements including the new one
        List<Element> allElements = new ArrayList<>();
        
        // Add existing elements
        for (int i = 0; i < leftPage.count(); i++) {
            allElements.add(leftPage.element(i));
        }
        
        // Add new element in sorted position
        Element newElement = new Element(newKey, newValue, false);
        boolean inserted = false;
        for (int i = 0; i < allElements.size(); i++) {
            if (compareKeys(newKey, allElements.get(i).key()) < 0) {
                allElements.add(i, newElement);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            allElements.add(newElement);
        }
        
        // Create new right page
        long rightPageId = pageManager.allocatePage();
        Page rightPage = pageManager.readPage(rightPageId);
        rightPage.setFlags(Page.FLAG_BRANCH);
        
        // Split elements between left and right pages
        int midPoint = allElements.size() / 2;
        
        // The middle element becomes the separator (promoted to parent)
        Element separatorElement = allElements.get(midPoint);
        
        // Clear left page and add first half of elements
        leftPage.setCount(0);
        for (int i = 0; i < midPoint; i++) {
            Element elem = allElements.get(i);
            leftPage.insert(elem.key(), elem.value(), elem.hasOverflow());
        }
        
        // Add second half to right page (excluding separator)
        for (int i = midPoint + 1; i < allElements.size(); i++) {
            Element elem = allElements.get(i);
            rightPage.insert(elem.key(), elem.value(), elem.hasOverflow());
        }
        
        // Write both pages
        pageManager.writePage(leftPage);
        pageManager.writePage(rightPage);
        
        System.out.println("   ✅ Split branch page " + leftPage.getPageId() + " -> " + 
                          leftPage.getPageId() + " + " + rightPageId + 
                          " (separator: " + new String(separatorElement.key()) + ")");
        
        return new SplitResult(leftPage.getPageId(), rightPageId, separatorElement.key());
    }
    
    /**
     * Creates a new root page when the old root splits
     */
    private void createNewRoot(SplitResult splitResult) throws IOException {
        System.out.println("   🌳 Creating new root page due to root split");
        
        long newRootId = pageManager.allocatePage();
        Page newRoot = pageManager.readPage(newRootId);
        newRoot.setFlags(Page.FLAG_BRANCH);
        
        // Insert pointers to left and right pages
        // First entry points to left page (no key needed for first entry in branch)
        byte[] leftPageIdBytes = ByteBuffer.allocate(8).putLong(splitResult.leftPageId).array();
        newRoot.insert(new byte[0], leftPageIdBytes); // Empty key for first entry
        
        // Second entry has separator key and points to right page
        byte[] rightPageIdBytes = ByteBuffer.allocate(8).putLong(splitResult.rightPageId).array();
        newRoot.insert(splitResult.separatorKey, rightPageIdBytes);
        
        pageManager.writePage(newRoot);
        
        // Update root page ID
        rootPageId = newRootId;
        
        System.out.println("   ✅ New root page created: " + newRootId);
    }
    
    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        System.out.println("🔍 BTree.read() - Reading key: " + new String(key));
        
        // Start at the root page
        long currentPageId = rootPageId;
        Page currentPage = pageManager.readPage(currentPageId);
        
        // Navigate to the leaf node
        while (currentPage.isBranch()) {
            int index = findChildIndex(currentPage, key);
            Element element = currentPage.element(index);
            
            ByteBuffer buffer = ByteBuffer.wrap(element.value());
            currentPageId = buffer.getLong();
            currentPage = pageManager.readPage(currentPageId);
        }
        
        // Search for the key in the leaf node
        int count = currentPage.count();
        for (int i = 0; i < count; i++) {
            Element element = currentPage.element(i);
            if (Arrays.equals(key, element.key())) {
                byte[] valueBytes;
                if (element.hasOverflow()) {
                    valueBytes = readFromOverflowPages(element.overflowPageId());
                } else {
                    valueBytes = element.value();
                }
                System.out.println("   ✅ Found key: " + new String(key));
                System.out.println();
                return Optional.of(valueSerializer.deserialize(valueBytes));
            }
        }
        
        System.out.println("   ❌ Key not found: " + new String(key));
        System.out.println();
        return Optional.empty();
    }
    
    @Override
    public List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        System.out.println("🔍 BTree.scan() - Scanning from '" + new String(startKey) + "' to '" + 
                          (endKey != null ? new String(endKey) : "END") + "'");
        
        List<Record> results = new ArrayList<>();
        
        // Start at the root page
        long currentPageId = rootPageId;
        Page currentPage = pageManager.readPage(currentPageId);
        
        // Navigate to the first leaf node that might contain startKey
        while (currentPage.isBranch()) {
            int index = findChildIndex(currentPage, startKey);
            Element element = currentPage.element(index);
            
            ByteBuffer buffer = ByteBuffer.wrap(element.value());
            currentPageId = buffer.getLong();
            currentPage = pageManager.readPage(currentPageId);
        }
        
        // Scan through leaf nodes
        while (currentPage != null) {
            int count = currentPage.count();
            
            for (int i = 0; i < count; i++) {
                Element element = currentPage.element(i);
                byte[] key = element.key();
                
                // Check if we're past the end of the range
                if (endKey != null && compareKeys(key, endKey) >= 0) {
                    System.out.println("   ✅ Scan completed. Found " + results.size() + " records");
                    System.out.println();
                    return results;
                }
                
                // Check if we're within the range (key >= startKey)
                if (compareKeys(key, startKey) >= 0) {
                    byte[] valueBytes;
                    if (element.hasOverflow()) {
                        valueBytes = readFromOverflowPages(element.overflowPageId());
                    } else {
                        valueBytes = element.value();
                    }
                    
                    Map<String, Object> value = valueSerializer.deserialize(valueBytes);
                    if (columns != null && !columns.isEmpty()) {
                        // Filter columns if specified
                        Map<String, Object> filteredValue = new HashMap<>();
                        for (String column : columns) {
                            if (value.containsKey(column)) {
                                filteredValue.put(column, value.get(column));
                            }
                        }
                        value = filteredValue;
                    }
                    
                    results.add(new Record(key, value));
                    System.out.println("   📄 Found record: " + new String(key));
                }
            }
            
            // Move to the next leaf node
            long nextPageId = currentPage.nextPageId();
            if (nextPageId == 0) {
                break;
            }
            try {
                currentPage = pageManager.readPage(nextPageId);
            } catch (IOException e) {
                // No more pages
                break;
            }
        }
        
        System.out.println("   ✅ Scan completed. Found " + results.size() + " records");
        System.out.println();
        return results;
    }
    
    @Override
    public void delete(byte[] key) throws IOException {
        // TODO: Implement delete operation
        // This method should:
        // 1. Navigate to the leaf page containing the key
        // 2. Remove the key-value pair from the leaf page
        // 3. Handle overflow pages if the value had them
        // 4. Implement page merging if the page becomes underutilized
        // 5. Update parent nodes if necessary
        // 6. Maintain B+Tree invariants
        throw new UnsupportedOperationException("Delete not implemented yet");
    }
    
    @Override
    public void writeBatch(List<Record> records) throws IOException {
        for (Record record : records) {
            write(record.getKey(), record.getValue());
        }
    }
    
    @Override
    public void close() throws IOException {
        pageManager.close();
    }
    
    // Helper methods...
    
    private int findChildIndex(Page page, byte[] key) {
        int count = page.count();
        
        // For branch pages, first entry might have empty key (points to leftmost child)
        for (int i = 0; i < count; i++) {
            Element element = page.element(i);
            byte[] elementKey = element.key();
            
            // Skip empty keys (first entry in branch page)
            if (elementKey.length == 0) {
                continue;
            }
            
            // If key is less than element key, go to previous child
            if (compareKeys(key, elementKey) < 0) {
                return Math.max(0, i - 1);
            }
        }
        
        // Key is greater than all keys, go to last child
        return Math.max(0, count - 1);
    }
    
    private int compareKeys(byte[] a, byte[] b) {
        return Arrays.compare(a, b);
    }
    
    private int calculateMaxInlineSize(Page leaf, byte[] key) {
        int freeSpace = leaf.freeSpace();
        int overhead = Page.ELEM_HEADER_SIZE + key.length;
        return freeSpace - overhead;
    }
    
    private boolean insertIntoLeaf(Page leaf, byte[] key, byte[] value) {
        return leaf.insert(key, value);
    }
    
    private boolean insertWithOverflow(Page leaf, byte[] key, byte[] value) throws IOException {
        long overflowPageId = createOverflowPages(value);
        return leaf.insert(key, ByteBuffer.allocate(8).putLong(overflowPageId).array(), true);
    }
    
    private long createOverflowPages(byte[] value) throws IOException {
        // TODO: Implement overflow page creation
        // This method should:
        // 1. Calculate number of pages needed for the value
        // 2. Allocate overflow pages sequentially
        // 3. Split the value across multiple pages
        // 4. Link pages together with next page IDs
        // 5. Return the ID of the first overflow page
        throw new UnsupportedOperationException("Overflow pages not implemented yet");
    }
    
    private void freeOverflowPages(long pageId) throws IOException {
        // TODO: Implement overflow page cleanup
        // This method should:
        // 1. Follow the chain of overflow pages
        // 2. Mark each page as free/available
        // 3. Clear page contents for security
        // 4. Update free page tracking
        throw new UnsupportedOperationException("Overflow page cleanup not implemented yet");
    }
    
    private byte[] readFromOverflowPages(long startPageId) throws IOException {
        // TODO: Implement overflow page reading
        // This method should:
        // 1. Follow the chain of overflow pages starting from startPageId
        // 2. Read data from each page in sequence
        // 3. Concatenate all data into a single byte array
        // 4. Return the complete value
        throw new UnsupportedOperationException("Overflow page reading not implemented yet");
    }
    
    /**
     * Prints page access statistics for educational purposes.
     */
    public void printPageAccessStatistics() {
        pageManager.printAccessStatistics();
    }
    
    /**
     * Resets page access counters for educational purposes.
     */
    public void resetPageAccessCounters() {
        pageManager.resetAccessCounters();
    }
    
    /**
     * Gets the total number of page reads.
     */
    public long getPageReadsCount() {
        return pageManager.getPageReadsCount();
    }
    
    /**
     * Gets the total number of page writes.
     */
    public long getPageWritesCount() {
        return pageManager.getPageWritesCount();
    }
    
    private static class ValueSerializer {
        private static final byte TYPE_NULL = 0;
        private static final byte TYPE_STRING = 1;
        private static final byte TYPE_INTEGER = 2;
        private static final byte TYPE_LONG = 3;
        private static final byte TYPE_DOUBLE = 4;
        private static final byte TYPE_BOOLEAN = 5;
        
        public byte[] serialize(Map<String, Object> value) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                
                // Write number of entries
                dos.writeInt(value.size());
                
                // Write each entry
                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    // Write key
                    byte[] keyBytes = entry.getKey().getBytes("UTF-8");
                    dos.writeInt(keyBytes.length);
                    dos.write(keyBytes);
                    
                    // Write value
                    Object val = entry.getValue();
                    if (val == null) {
                        dos.writeByte(TYPE_NULL);
                    } else if (val instanceof String) {
                        dos.writeByte(TYPE_STRING);
                        byte[] valBytes = ((String) val).getBytes("UTF-8");
                        dos.writeInt(valBytes.length);
                        dos.write(valBytes);
                    } else if (val instanceof Integer) {
                        dos.writeByte(TYPE_INTEGER);
                        dos.writeInt((Integer) val);
                    } else if (val instanceof Long) {
                        dos.writeByte(TYPE_LONG);
                        dos.writeLong((Long) val);
                    } else if (val instanceof Double) {
                        dos.writeByte(TYPE_DOUBLE);
                        dos.writeDouble((Double) val);
                    } else if (val instanceof Boolean) {
                        dos.writeByte(TYPE_BOOLEAN);
                        dos.writeBoolean((Boolean) val);
                    } else {
                        throw new IllegalArgumentException("Unsupported value type: " + val.getClass());
                    }
                }
                
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize value", e);
            }
        }
        
        public Map<String, Object> deserialize(byte[] bytes) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 DataInputStream dis = new DataInputStream(bais)) {
                
                Map<String, Object> value = new HashMap<>();
                
                // Read number of entries
                int numEntries = dis.readInt();
                
                // Read each entry
                for (int i = 0; i < numEntries; i++) {
                    // Read key
                    int keyLength = dis.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, "UTF-8");
                    
                    // Read value
                    byte type = dis.readByte();
                    Object val;
                    
                    switch (type) {
                        case TYPE_NULL:
                            val = null;
                            break;
                        case TYPE_STRING:
                            int valLength = dis.readInt();
                            byte[] valBytes = new byte[valLength];
                            dis.readFully(valBytes);
                            val = new String(valBytes, "UTF-8");
                            break;
                        case TYPE_INTEGER:
                            val = dis.readInt();
                            break;
                        case TYPE_LONG:
                            val = dis.readLong();
                            break;
                        case TYPE_DOUBLE:
                            val = dis.readDouble();
                            break;
                        case TYPE_BOOLEAN:
                            val = dis.readBoolean();
                            break;
                        default:
                            throw new IOException("Unknown value type: " + type);
                    }
                    
                    value.put(key, val);
                }
                
                return value;
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize value", e);
            }
        }
    }
} 