package minispark.storage.perf;

import minispark.util.TestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Assignment 1: Find out throughput of your disk.
 * 
 * This test measures disk write performance by continuously writing data
 * for a specified duration and calculating throughput metrics.
 * 
 * Key concepts demonstrated:
 * - Sequential disk writes
 * - Throughput measurement (MB/s)
 * - File system buffering vs physical disk sync
 * - Performance bottlenecks in storage systems
 * 
 * Try running this test with and without the sync() call to see the difference
 * between writing to OS buffer vs forcing writes to physical storage.
 */
public class DiskWritePerformanceTest {

    private static final String FILE_NAME = "testfile.bin";
    private static final int WRITE_SIZE = 1024; // Size of each write in bytes (1 KB)
    private static final int DURATION_IN_SECONDS = 20; // Duration of the test in seconds

    public static void main(String[] args) throws IOException {
        System.out.println("=== Disk Write Performance Test ===");
        System.out.println("This test measures your disk's write throughput");
        System.out.println("Write size: " + WRITE_SIZE + " bytes");
        System.out.println("Test duration: " + DURATION_IN_SECONDS + " seconds");
        System.out.println();

        byte[] data = createData(WRITE_SIZE);
        File perfFile = createFile(FILE_NAME);

        System.out.println("Writing data to: " + perfFile.getAbsolutePath());
        System.out.println("Starting performance test...");
        System.out.println();

        PerformanceMetrics metrics = performWriteTest(perfFile, data, DURATION_IN_SECONDS);

        printMetrics(metrics);
        
        // Clean up
        if (perfFile.exists()) {
            perfFile.delete();
        }
    }

    private static byte[] createData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = 'A';
        }
        return data;
    }

    private static File createFile(String fileName) {
        return new File(TestUtils.tempDir("perf"), fileName);
    }

    private static PerformanceMetrics performWriteTest(File file, byte[] data, int durationInSeconds) throws IOException {
        Instant startTime = Instant.now();
        Instant endTime = startTime.plus(Duration.ofSeconds(durationInSeconds));

        long numberOfWrites = writeUntil(endTime, file, data);

        Instant actualEndTime = Instant.now();
        Duration duration = Duration.between(startTime, actualEndTime);

        return new PerformanceMetrics(numberOfWrites, duration.getSeconds(), WRITE_SIZE);
    }

    private static long writeUntil(Instant endTime, File file, byte[] data) throws IOException {
        long numberOfWrites = 0;
        try (FileOutputStream os = new FileOutputStream(file)) {
            while (Instant.now().isBefore(endTime)) {
                //insert into customer ("1", "Name");
                os.write(data);//No Guarantee that this data is written to hard disk.
                //return success to user
                os.flush();
                //crash at this point.
                
                // EXPERIMENT: Uncomment the line below to force writes to physical storage
                // This will show the difference between OS buffer writes vs actual disk writes
                 syncToPhysicalMedia(os);

                numberOfWrites++;
                
                // Print progress every 1000 writes
                if (numberOfWrites % 1000 == 0) {
                    System.out.print(".");
                }
            }
        }
        System.out.println(); // New line after progress dots
        return numberOfWrites;
    }

    private static void syncToPhysicalMedia(FileOutputStream os) throws IOException {
        // Uncomment the line below to force synchronization to physical storage
        // This ensures data is written to disk, not just OS buffers
         os.getFD().sync(); // Why is this important for crash recovery?
    }

    private static void printMetrics(PerformanceMetrics metrics) {
        System.out.println("=== Performance Results ===");
        
        double writesPerSecond = metrics.getNumberOfWrites() / metrics.getSeconds();
        double mbWritten = (metrics.getNumberOfWrites() * metrics.getWriteSize()) / (1024.0 * 1024.0);
        double mbPerSecond = mbWritten / metrics.getSeconds();

        System.out.printf("Total writes: %,d%n", metrics.getNumberOfWrites());
        System.out.printf("Total time: %.2f seconds%n", metrics.getSeconds());
        System.out.printf("Writes per second: %,.0f%n", writesPerSecond);
        System.out.printf("MB written: %.2f MB%n", mbWritten);
        System.out.printf("Throughput: %.2f MB/s%n", mbPerSecond);
        
        System.out.println();
        System.out.println("=== Assignment Questions ===");
        System.out.println("1. What happens to throughput when you enable sync() calls?");
        System.out.println("2. Why is there such a big difference?");
        System.out.println("3. What are the trade-offs between performance and durability?");
        System.out.println("4. How does this relate to database write-ahead logs?");
    }

    private static class PerformanceMetrics {
        private final long numberOfWrites;
        private final double seconds;
        private final int writeSize;

        public PerformanceMetrics(long numberOfWrites, double seconds, int writeSize) {
            this.numberOfWrites = numberOfWrites;
            this.seconds = seconds;
            this.writeSize = writeSize;
        }

        public long getNumberOfWrites() {
            return numberOfWrites;
        }

        public double getSeconds() {
            return seconds;
        }

        public int getWriteSize() {
            return writeSize;
        }
    }
} 