package minispark.core;

import minispark.MiniSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class MiniRDDTest {
    private MiniSparkContext sc;

    @BeforeEach
    void setUp() {
        sc = new MiniSparkContext(2); // Use 2 partitions for testing
    }
    
    @Test
    void shouldTransformDataUsingMapAndFilter() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        MiniRDD<Integer> rdd = sc.parallelize(data);
        
        List<String> result = rdd
            .filter(num -> num % 2 == 0)
            .map(String::valueOf)
            .collect();
            
        assertEquals(Arrays.asList("2", "4"), result);
    }

    @Test
    void shouldHandleEmptyRDD() {
        List<Integer> data = new ArrayList<>();
        MiniRDD<Integer> rdd = sc.parallelize(data);
        
        List<Integer> result = rdd
            .filter(x -> x > 0)
            .collect();
            
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldPreserveLaziness() {
        List<Integer> data = Arrays.asList(1, 2, 3);
        MiniRDD<Integer> rdd = sc.parallelize(data);
        
        // These transformations should not be executed yet
        MiniRDD<Integer> filtered = rdd.filter(x -> {
            fail("Filter should not be executed until collect()");
            return true;
        });
        
        MiniRDD<String> mapped = filtered.map(x -> {
            fail("Map should not be executed until collect()");
            return x.toString();
        });
        
        // No assertions needed - if the lambdas are executed, the test will fail
    }

    @Test
    void shouldPreserveTransformationOrder() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        MiniRDD<Integer> rdd = sc.parallelize(data);
        
        List<Integer> result = rdd
            .map(x -> x * 2)      // [2, 4, 6, 8, 10]
            .filter(x -> x > 5)   // [6, 8, 10]
            .map(x -> x / 2)      // [3, 4, 5]
            .collect();
            
        assertEquals(Arrays.asList(3, 4, 5), result);
    }

    @Test
    void shouldHandleNullValues() {
        List<String> data = Arrays.asList("a", null, "b", null, "c");
        MiniRDD<String> rdd = sc.parallelize(data);
        
        List<String> result = rdd
            .filter(s -> s != null)
            .map(String::toUpperCase)
            .collect();
            
        assertEquals(Arrays.asList("A", "B", "C"), result);
    }

    @Test
    void shouldChainMultipleFilters() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        MiniRDD<Integer> rdd = sc.parallelize(data);
        
        List<Integer> result = rdd
            .filter(x -> x % 2 == 0)  // [2, 4, 6]
            .filter(x -> x > 2)       // [4, 6]
            .filter(x -> x < 6)       // [4]
            .collect();
            
        assertEquals(Arrays.asList(4), result);
    }
} 