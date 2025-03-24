package minispark.core;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MiniRDDTest {
    
    @Test
    void shouldTransformDataUsingMapAndFilter() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        ParallelCollectionRDD<Integer> rdd = new ParallelCollectionRDD<>(data);
        
        List<String> result = rdd
            .filter(num -> num % 2 == 0)
            .map(String::valueOf)
            .collect();
            
        assertEquals(Arrays.asList("2", "4"), result);
    }
} 