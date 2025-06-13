package minispark.storage.btree.page;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a page in the B+Tree.
 * A page can be either a leaf node or a branch node.
 */
public class Page {
    // Page flags
    public static final int FLAG_LEAF = 0x01;
    public static final int FLAG_BRANCH = 0x02;
    public static final int FLAG_OVERFLOW = 0x04;
    
    // Page header size in bytes
    public static final int PAGE_HEADER_SIZE = 16;
    
    // Element header size in bytes
    public static final int ELEM_HEADER_SIZE = 8;
    
    // Element size in bytes (header + key + value)
    public static final int ELEM_SIZE = 32;
    
    // Page data
    private final ByteBuffer buffer;
    private final int pageSize;
    private final long pageId;
    
    /**
     * Creates a new page with the given size and ID.
     *
     * @param pageSize Size of the page in bytes
     * @param pageId The page ID
     */
    public Page(int pageSize, long pageId) {
        this.pageSize = pageSize;
        this.pageId = pageId;
        this.buffer = ByteBuffer.allocate(pageSize);
        // Initialize with zeros
        buffer.clear();
        for (int i = 0; i < pageSize; i++) {
            buffer.put((byte) 0);
        }
        buffer.rewind();
    }
    
    /**
     * Gets the page ID.
     *
     * @return The page ID
     */
    public long getPageId() {
        return pageId;
    }
    
    /**
     * Gets the page flags.
     *
     * @return The page flags
     */
    public int flags() {
        return buffer.getInt(0);
    }
    
    /**
     * Sets the page flags.
     *
     * @param flags The page flags
     */
    public void setFlags(int flags) {
        buffer.putInt(0, flags);
    }
    
    /**
     * Checks if this is a leaf node.
     *
     * @return true if this is a leaf node
     */
    public boolean isLeaf() {
        return (flags() & FLAG_LEAF) != 0;
    }
    
    /**
     * Checks if this is a branch node.
     *
     * @return true if this is a branch node
     */
    public boolean isBranch() {
        return (flags() & FLAG_BRANCH) != 0;
    }
    
    /**
     * Gets the number of elements in the page.
     *
     * @return The number of elements
     */
    public int count() {
        return buffer.getInt(4);
    }
    
    /**
     * Sets the number of elements in the page.
     *
     * @param count The number of elements
     */
    public void setCount(int count) {
        buffer.putInt(4, count);
    }
    
    /**
     * Clears all elements from the page.
     */
    public void clear() {
        setCount(0);
        // Clear the data area (optional, for security)
        for (int i = PAGE_HEADER_SIZE; i < pageSize; i++) {
            buffer.put(i, (byte) 0);
        }
    }
    
    /**
     * Gets the ID of the next page (for leaf nodes).
     *
     * @return The next page ID
     */
    public long nextPageId() {
        return buffer.getLong(8);
    }
    
    /**
     * Sets the ID of the next page (for leaf nodes).
     *
     * @param pageId The next page ID
     */
    public void setNextPageId(long pageId) {
        buffer.putLong(8, pageId);
    }
    
    /**
     * Gets the free space in the page.
     *
     * @return The free space in bytes
     */
    public int freeSpace() {
        int usedSpace = PAGE_HEADER_SIZE;
        int count = count();
        
        // Calculate actual used space by examining each element
        // Use safe element access to avoid BufferUnderflowException
        for (int i = 0; i < count; i++) {
            try {
                Element element = elementSafe(i);
                if (element != null) {
                    usedSpace += ELEM_HEADER_SIZE + element.key().length + element.value().length;
                } else {
                    // If we can't read an element safely, assume page is corrupted
                    // Return minimal free space to prevent further insertions
                    return 0;
                }
            } catch (Exception e) {
                // If we encounter any error reading elements, assume page is full
                return 0;
            }
        }
        
        return pageSize - usedSpace;
    }
    
    /**
     * Gets an element at the given index with bounds checking.
     *
     * @param index The element index
     * @return The element, or null if bounds are exceeded
     */
    public Element elementSafe(int index) {
        if (index >= count() || index < 0) {
            return null;
        }
        
        try {
            // Find the actual offset of this element
            int offset = PAGE_HEADER_SIZE;
            for (int i = 0; i < index; i++) {
                // Validate we have enough space for element header
                if (offset + ELEM_HEADER_SIZE > pageSize) {
                    return null;
                }
                
                int keyLength = buffer.getShort(offset);
                int valueLength = buffer.getShort(offset + 2);
                
                // Validate lengths are reasonable (more lenient bounds)
                if (keyLength < 0 || valueLength < 0 || 
                    keyLength > pageSize - ELEM_HEADER_SIZE || valueLength > pageSize - ELEM_HEADER_SIZE) {
                    return null;
                }
                
                // Validate the complete element fits in the page
                if (offset + ELEM_HEADER_SIZE + keyLength + valueLength > pageSize) {
                    return null;
                }
                
                offset += ELEM_HEADER_SIZE + keyLength + valueLength;
            }
            
            // Validate we have enough space for the target element header
            if (offset + ELEM_HEADER_SIZE > pageSize) {
                return null;
            }
            
            int keyLength = buffer.getShort(offset);
            int valueLength = buffer.getShort(offset + 2);
            
            // Validate lengths are reasonable (more lenient bounds)
            if (keyLength < 0 || valueLength < 0 || 
                keyLength > pageSize - ELEM_HEADER_SIZE || valueLength > pageSize - ELEM_HEADER_SIZE) {
                return null;
            }
            
            // Validate the complete element fits in the page
            if (offset + ELEM_HEADER_SIZE + keyLength + valueLength > pageSize) {
                return null;
            }
            
            boolean hasOverflow = (buffer.get(offset + 4) & 0x01) != 0;
            
            byte[] key = new byte[keyLength];
            buffer.position(offset + 8);
            buffer.get(key);
            
            byte[] value = new byte[valueLength];
            buffer.get(value);
            
            return new Element(key, value, hasOverflow);
        } catch (Exception e) {
            // Any buffer operation failed - return null
            return null;
        }
    }

    /**
     * Gets an element at the given index.
     *
     * @param index The element index
     * @return The element
     */
    public Element element(int index) {
        if (index >= count()) {
            throw new IndexOutOfBoundsException("Element index " + index + " >= count " + count());
        }
        
        try {
            // Find the actual offset of this element
            int offset = PAGE_HEADER_SIZE;
            for (int i = 0; i < index; i++) {
                // Add bounds checking to prevent BufferUnderflowException
                if (offset + ELEM_HEADER_SIZE > pageSize) {
                    throw new IndexOutOfBoundsException("Element offset " + offset + " exceeds page size " + pageSize);
                }
                
                int keyLength = buffer.getShort(offset);
                int valueLength = buffer.getShort(offset + 2);
                
                // Validate element size is reasonable
                if (keyLength < 0 || valueLength < 0) {
                    throw new IndexOutOfBoundsException("Invalid element lengths: key=" + keyLength + ", value=" + valueLength);
                }
                
                offset += ELEM_HEADER_SIZE + keyLength + valueLength;
                
                // Ensure we don't go beyond page bounds
                if (offset > pageSize) {
                    throw new IndexOutOfBoundsException("Element extends beyond page bounds");
                }
            }
            
            // Validate target element header fits
            if (offset + ELEM_HEADER_SIZE > pageSize) {
                throw new IndexOutOfBoundsException("Target element header exceeds page size");
            }
            
            int keyLength = buffer.getShort(offset);
            int valueLength = buffer.getShort(offset + 2);
            
            // Validate target element lengths
            if (keyLength < 0 || valueLength < 0) {
                throw new IndexOutOfBoundsException("Invalid target element lengths: key=" + keyLength + ", value=" + valueLength);
            }
            
            // Validate target element data fits
            if (offset + ELEM_HEADER_SIZE + keyLength + valueLength > pageSize) {
                throw new IndexOutOfBoundsException("Target element data exceeds page size");
            }
            
            boolean hasOverflow = (buffer.get(offset + 4) & 0x01) != 0;
            
            byte[] key = new byte[keyLength];
            buffer.position(offset + 8);
            buffer.get(key);
            
            byte[] value = new byte[valueLength];
            buffer.get(value);
            
            return new Element(key, value, hasOverflow);
            
        } catch (java.nio.BufferUnderflowException e) {
            throw new IndexOutOfBoundsException("BufferUnderflowException reading element " + index + 
                ": page data may be corrupted. Page size: " + pageSize + ", count: " + count());
        }
    }
    
    /**
     * Inserts a key-value pair into the page.
     *
     * @param key The key
     * @param value The value
     * @return true if the insertion was successful
     */
    public boolean insert(byte[] key, byte[] value) {
        return insert(key, value, false);
    }
    
    /**
     * Inserts a key-value pair into the page.
     *
     * @param key The key
     * @param value The value
     * @param hasOverflow Whether the value has overflow pages
     * @return true if the insertion was successful
     */
    public boolean insert(byte[] key, byte[] value, boolean hasOverflow) {
        // Validate input parameters
        if (key == null || value == null) {
            return false;
        }
        
        // Check if there's enough space
        int requiredSpace = ELEM_HEADER_SIZE + key.length + value.length;
        if (requiredSpace > freeSpace()) {
            return false;
        }
        
        // Find insertion position (keep keys sorted)
        int insertPos = 0;
        int count = count();
        for (int i = 0; i < count; i++) {
            Element element = elementSafe(i);
            if (element == null) {
                // Page appears corrupted, cannot safely insert
                return false;
            }
            
            int cmp = Arrays.compare(key, element.key());
            if (cmp < 0) {
                insertPos = i;
                break;
            } else if (cmp == 0) {
                // Key already exists, update it
                // For simplicity, we'll just append for now
                insertPos = count;
                break;
            }
            insertPos = i + 1;
        }
        
        // Calculate where to insert
        int insertOffset = PAGE_HEADER_SIZE;
        for (int i = 0; i < insertPos; i++) {
            Element element = elementSafe(i);
            if (element == null) {
                // Page appears corrupted, cannot safely insert
                return false;
            }
            insertOffset += ELEM_HEADER_SIZE + element.key().length + element.value().length;
        }
        
        // Validate insertion offset is within bounds
        if (insertOffset + requiredSpace > pageSize) {
            return false;
        }
        
        // Shift existing elements if needed
        if (insertPos < count) {
            int shiftStart = insertOffset;
            int shiftEnd = insertOffset;
            
            // Find the end of used space
            for (int i = insertPos; i < count; i++) {
                Element element = elementSafe(i);
                if (element == null) {
                    // Page appears corrupted, cannot safely insert
                    return false;
                }
                shiftEnd += ELEM_HEADER_SIZE + element.key().length + element.value().length;
            }
            
            // Validate shift operation is safe
            if (shiftEnd - shiftStart < 0 || shiftStart + requiredSpace + (shiftEnd - shiftStart) > pageSize) {
                return false;
            }
            
            // Shift data
            byte[] temp = new byte[shiftEnd - shiftStart];
            buffer.position(shiftStart);
            buffer.get(temp);
            buffer.position(shiftStart + requiredSpace);
            buffer.put(temp);
        }
        
        // Insert new element
        buffer.putShort(insertOffset, (short) key.length);
        buffer.putShort(insertOffset + 2, (short) value.length);
        buffer.put(insertOffset + 4, (byte) (hasOverflow ? 0x01 : 0x00));
        buffer.put(insertOffset + 5, (byte) 0); // padding
        buffer.putShort(insertOffset + 6, (short) 0); // padding
        buffer.position(insertOffset + 8);
        buffer.put(key);
        buffer.put(value);
        
        // Update count
        setCount(count + 1);
        
        return true;
    }
    
    /**
     * Gets the raw page data.
     *
     * @return The page data
     */
    public byte[] getData() {
        return buffer.array();
    }
    
    /**
     * Sets the raw page data.
     *
     * @param data The page data
     */
    public void setData(byte[] data) {
        buffer.clear();
        buffer.put(data);
        buffer.rewind();
    }
} 