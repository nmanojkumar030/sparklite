package minispark.storage.btree;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles serialization and deserialization of values.
 */
public class ValueSerializer {
    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_INTEGER = 2;
    private static final byte TYPE_LONG = 3;
    private static final byte TYPE_DOUBLE = 4;
    private static final byte TYPE_BOOLEAN = 5;
    
    /**
     * Serializes a value map to bytes.
     *
     * @param value The value map to serialize
     * @return The serialized bytes
     */
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
    
    /**
     * Deserializes bytes to a value map.
     *
     * @param bytes The bytes to deserialize
     * @return The deserialized value map
     */
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