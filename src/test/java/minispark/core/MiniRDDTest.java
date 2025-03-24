package minispark.core;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class MiniRDDTest {
    
    @Test
    void shouldTransformDataUsingMapAndFilter() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        CollectionRDD<Integer> rdd = new CollectionRDD<>(data);
        
        List<String> result = rdd
            .filter(num -> num % 2 == 0)
            .map(String::valueOf)
            .collect();
            
        assertEquals(Arrays.asList("2", "4"), result);
    }

    @Test
    void shouldHandleEmptyRDD() {
        List<Integer> data = new ArrayList<>();
        CollectionRDD<Integer> rdd = new CollectionRDD<>(data);
        
        List<Integer> result = rdd
            .filter(x -> x > 0)
            .collect();
            
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldPreserveLaziness() {
        List<Integer> data = Arrays.asList(1, 2, 3);
        CollectionRDD<Integer> rdd = new CollectionRDD<>(data);
        
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
        CollectionRDD<Integer> rdd = new CollectionRDD<>(data);
        
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
        CollectionRDD<String> rdd = new CollectionRDD<>(data);
        
        List<String> result = rdd
            .filter(s -> s != null)
            .map(String::toUpperCase)
            .collect();
            
        assertEquals(Arrays.asList("A", "B", "C"), result);
    }

    @Test
    void shouldChainMultipleFilters() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        CollectionRDD<Integer> rdd = new CollectionRDD<>(data);
        
        List<Integer> result = rdd
            .filter(x -> x % 2 == 0)  // [2, 4, 6]
            .filter(x -> x > 2)       // [4, 6]
            .filter(x -> x < 6)       // [4]
            .collect();
            
        assertEquals(Arrays.asList(4), result);
    }
} 