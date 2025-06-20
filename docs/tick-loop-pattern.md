# Tick Loop Pattern for Deterministic Simulation

## Overview

The Tick Loop Pattern is a deterministic simulation approach inspired by TigerBeetle's architecture. It ensures that all asynchronous operations progress through controlled tick cycles, making the system completely deterministic and reproducible.

## Expert Review & Determinism Fixes

Based on expert review, we've implemented the following critical determinism improvements:

### 1. **Single Logical Thread** ✅ FIXED
**Issue**: Workers called `Thread.sleep(1)` which yields to OS scheduler and breaks determinism under load.

**Solution**: Removed all `Thread.sleep()` calls from Worker.executeTaskWithTickProgression(). Pure tick progression ensures deterministic execution order.

```java
// ❌ Before - Non-deterministic
while (!future.isDone()) {
    messageBus.tick();
    Thread.sleep(1); // Breaks determinism!
}

// ✅ After - Deterministic  
while (!future.isDone()) {
    messageBus.tick(); // Pure tick progression
}
```

### 2. **Reproducible Randomness** ✅ FIXED
**Issue**: `SimulatedNetwork` used `new Random()` and `ThreadLocalRandom.current()` without seeded randomness.

**Solution**: Accept seed in constructor, use seeded Random instance for all randomness.

```java
// ✅ Deterministic Random Usage
public SimulatedNetwork(BiConsumer<MessageEnvelope, DeliveryContext> callback, long randomSeed) {
    this.random = new Random(randomSeed); // Seeded for reproducibility
}

private long calculateDeliveryTick() {
    // Use seeded random instead of ThreadLocalRandom
    delay = minLatencyTicks + random.nextInt(maxLatencyTicks - minLatencyTicks + 1);
}
```

### 3. **Deterministic Data Structures** ✅ FIXED
**Issue**: `ConcurrentHashMap`, `HashMap`, `HashSet` have non-deterministic iteration order that changed between JDKs.

**Solution**: Replaced with `LinkedHashMap`/`LinkedHashSet` for deterministic iteration order.

```java
// ❌ Before - Non-deterministic iteration
private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
private final Set<Long> points = new HashSet<>();

// ✅ After - Deterministic iteration
private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
private final Set<Long> points = new LinkedHashSet<>();
```

**Files Updated**:
- `MessageBus`: HashMap → LinkedHashMap
- `TaskSchedulerImpl`: ConcurrentHashMap → LinkedHashMap  
- `DAGScheduler`: HashMap → LinkedHashMap, HashSet → LinkedHashSet
- `Client`: ConcurrentHashMap → LinkedHashMap
- `SimulatedNetwork`: ConcurrentHashMap → LinkedHashMap, HashSet → LinkedHashSet
- `HashRing`: ConcurrentHashMap → LinkedHashMap, ConcurrentSkipListMap → TreeMap

### 4. **Centralized Tick Management** ✅ ADDED
**Enhancement**: Created `SimulationRunner` for centralized tick progression following expert recommendation.

```java
public class SimulationRunner {
    public int tick() {
        // Deterministic tick order: Scheduler → Workers → Network
        messageBus.tick();
        return messagesDelivered;
    }
    
    public void runUntil(BooleanSupplier condition, Duration timeout) {
        // Replaces Thread.sleep() with pure tick progression
        while (!condition.getAsBoolean()) {
            tick();
        }
    }
}
```

## Additional Determinism Hot Spots Fixed

Based on expert review, we identified and fixed these critical remaining issues:

### 5. **Async Executor Usage** ✅ FIXED
**Issue**: `CompletableFuture.supplyAsync()` uses default executor which is non-deterministic.

**Solution**: Replace with `new CompletableFuture<>()` and manual completion under tick control.

```java
// ❌ Before - Non-deterministic executor
CompletableFuture<FilePartition[]> future = CompletableFuture.supplyAsync(() -> {
    return objectFileReader.createPartitions(key, partitions);
});

// ✅ After - Manual completion under tick control  
CompletableFuture<FilePartition[]> future = new CompletableFuture<>();
try {
    FilePartition[] partitions = objectFileReader.createPartitions(key, partitions);
    future.complete(partitions);
} catch (Exception e) {
    future.completeExceptionally(e);
}
```

### 6. **Blocking Future Operations** ✅ FIXED
**Issue**: `CompletableFuture::join()` blocks on real time instead of tick progression.

**Solution**: Replace join() with async composition or tick-driven completion.

```java
// ❌ Before - Blocks on real time
return futures.stream()
    .map(CompletableFuture::join)  // Breaks determinism!
    .collect(toList());

// ✅ After - Async composition
return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
    .thenApply(v -> {
        List<String> results = new ArrayList<>();
        for (CompletableFuture<String> future : futures) {
            results.addAll(future.get()); // Safe after allOf()
        }
        return results;
    });
```

### 7. **Test-Time Blocking** ✅ FIXED  
**Issue**: Tests call `result.join()` inside JUnit thread, blocking on real time.

**Solution**: Use `TestUtils.runUntil()` to drive ticks until completion.

```java
// ❌ Before - Blocks on real time in tests
CompletableFuture<FilePartition[]> future = createPartitions();
FilePartition[] result = future.join(); // Breaks determinism!

// ✅ After - Tick-driven completion in tests
CompletableFuture<FilePartition[]> future = createPartitions();
TestUtils.runUntil(messageBus, () -> future.isDone(), Duration.ofSeconds(10));
FilePartition[] result = future.get(); // Safe after tick progression
```

**Files Updated**:
- `ObjectFileParquetReaderTest`: supplyAsync → manual completion
- `Client`: CompletableFuture::join → async composition
- Tests: Added tick progression instead of blocking waits

## Core Principles

### 1. Single Tick Progression Point
**Key Learning**: Only one place in the system should drive MessageBus ticks. This prevents deadlocks and ensures deterministic execution.

- **✅ Correct**: Worker drives ticks until task futures complete
- **❌ Wrong**: Multiple places calling `messageBus.tick()` 
- **❌ Wrong**: Blocking on futures without tick progression

### 2. Async Interfaces with Centralized Execution
All components should expose async interfaces (returning `CompletableFuture`) but have a single execution context that drives tick progression:

```java
// RDD Interface - Always Async
public interface MiniRDD<T> {
    CompletableFuture<Iterator<T>> compute(Partition partition);
    CompletableFuture<List<T>> collect();
}

// Worker - Single Tick Progression Point
public class Worker {
    private Object executeTaskWithTickProgression(Task<Object, Object> task, Partition partition) {
        CompletableFuture<Object> future = task.execute(partition);
        
        // Drive ticks until completion - ONLY place that should do this
        while (!future.isDone()) {
            messageBus.tick();
        }
        
        return future.get(); // Safe after isDone()
    }
}
```

### 3. Test-Time Tick Progression
For tests that don't go through the Worker execution path, use `TestUtils.runUntil()`:

```java
// ✅ Correct - Use TestUtils for test-specific tick progression
CompletableFuture<Void> future = client.putObject(key, data);
minispark.util.TestUtils.runUntil(messageBus,
    () -> future.isDone(),
    Duration.ofSeconds(5));

// ❌ Wrong - Blocking without tick progression
client.putObject(key, data).join(); // Will hang forever!
```

## Architecture Benefits

### 1. Clean Separation of Concerns
- **RDDs**: Define async computation logic
- **Worker**: Handles tick progression and execution
- **Tests**: Use TestUtils for controlled tick progression

### 2. Scalability
- One worker process per core
- Each worker runs single-threaded event loop
- No thread synchronization issues

### 3. Deterministic Execution
- All async operations progress through controlled ticks
- Reproducible behavior across runs
- No race conditions or timing issues

## Common Anti-Patterns and Fixes

### Anti-Pattern 1: Blocking in RDD.collect()
```java
// ❌ Wrong - RDD blocking on futures
public CompletableFuture<List<T>> collect() {
    List<CompletableFuture<Iterator<T>>> futures = new ArrayList<>();
    for (Partition partition : getPartitions()) {
        futures.add(compute(partition));
    }
    
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> {
            List<T> result = new ArrayList<>();
            for (CompletableFuture<Iterator<T>> future : futures) {
                Iterator<T> iter = future.get(); // ❌ BLOCKS!
                // ...
            }
            return result;
        });
}

// ✅ Correct - Use join() after allOf() completes
public CompletableFuture<List<T>> collect() {
    // ... same setup ...
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> {
            List<T> result = new ArrayList<>();
            for (CompletableFuture<Iterator<T>> future : futures) {
                Iterator<T> iter = future.join(); // ✅ Safe after allOf()
                // ...
            }
            return result;
        });
}
```

### Anti-Pattern 2: Blocking in Tests
```java
// ❌ Wrong - Test blocking without tick progression
@Test
void testObjectStore() {
    client.putObject("key", data).join(); // Will hang!
}

// ✅ Correct - Test with tick progression
@Test
void testObjectStore() {
    CompletableFuture<Void> future = client.putObject("key", data);
    minispark.util.TestUtils.runUntil(messageBus,
        () -> future.isDone(),
        Duration.ofSeconds(5));
}
```

### Anti-Pattern 3: Mixed Sync/Async Task Interface
```java
// ❌ Wrong - Task returning sync result
public abstract class Task<I, O> {
    public abstract O execute(Partition partition); // Sync return
}

// ✅ Correct - Task returning async result
public abstract class Task<I, O> {
    public abstract CompletableFuture<O> execute(Partition partition); // Async return
}
```

## Implementation Examples

### Worker Execution Pattern
```java
public class Worker {
    private Object executeTaskWithTickProgression(Task<Object, Object> task, Partition partition) {
        // Execute task - returns CompletableFuture
        CompletableFuture<Object> future = task.execute(partition);
        
        // Drive ticks until completion - SINGLE PLACE for tick progression
        while (!future.isDone()) {
            messageBus.tick();
        }
        
        try {
            return future.get(); // Safe to get() after isDone()
        } catch (Exception e) {
            throw new RuntimeException("Task future failed", e);
        }
    }
}
```

### Test Utility Pattern
```java
public class TestUtils {
    public static void runUntil(MessageBus messageBus, BooleanSupplier condition, Duration timeout) {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeout.toMillis();
        
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new RuntimeException("Timeout waiting for condition");
            }
            
            messageBus.tick();
            
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting", e);
            }
        }
    }
}
```

## Debugging Hanging Tests

If a test hangs, check for these common issues:

1. **Missing tick progression**: Look for `.join()` or `.get()` calls without corresponding `TestUtils.runUntil()`
2. **Multiple tick drivers**: Ensure only one place is calling `messageBus.tick()`
3. **Blocking in async chains**: Use `.thenApply()` instead of `.get()` in CompletableFuture chains

## Lessons Learned

1. **Centralized tick progression prevents deadlocks**: Having multiple places drive ticks can cause race conditions
2. **Async interfaces enable composability**: RDDs can be chained without blocking
3. **Single-threaded execution simplifies reasoning**: No need for complex synchronization
4. **Test utilities are essential**: Tests need their own tick progression mechanism
5. **Client-as-Peer Architecture**: In message-passing systems, clients are peers that need response handlers

### Client-as-Peer Pattern

A key insight from our implementation is that in a proper message-passing distributed system, **clients are not just request senders - they are peers in the network that must handle responses**.

#### Traditional Client-Server vs Message-Passing Architecture

```java
// ❌ Traditional blocking client-server model
class TraditionalClient {
    public String getValue(String key) {
        // Synchronous request-response
        return httpClient.get("/api/get?key=" + key);
    }
}

// ✅ Message-passing peer model
class MessagePassingClient implements MessageBus.MessageHandler {
    private final Map<String, CompletableFuture<String>> pendingRequests = new HashMap<>();
    
    public CompletableFuture<String> getValue(String key) {
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<String> future = new CompletableFuture<>();
        
        // Store the future to complete when response arrives
        pendingRequests.put(requestId, future);
        
        // Send async message
        GetRequest request = new GetRequest(requestId, key);
        messageBus.send(request, clientEndpoint, serverEndpoint);
        
        return future; // Non-blocking return
    }
    
    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        if (message instanceof GetResponse response) {
            // Complete the corresponding future when response arrives
            CompletableFuture<String> future = pendingRequests.remove(response.requestId());
            if (future != null) {
                future.complete(response.value());
            }
        }
    }
}
```

#### Why This Matters

1. **Symmetric Architecture**: Both clients and servers are message handlers registered with the MessageBus
2. **Non-blocking Operations**: Clients return futures immediately and complete them when responses arrive
3. **Request Correlation**: Clients must track pending requests and match them with incoming responses
4. **Deterministic Testing**: All communication flows through the same tick-driven message bus

#### Implementation Example from Our System

```java
// Client registration - just like servers
public class Client implements MessageBus.MessageHandler {
    private final MessageBus messageBus;
    private final NetworkEndpoint clientEndpoint;
    private final Map<String, CompletableFuture<byte[]>> pendingGets = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Void>> pendingPuts = new ConcurrentHashMap<>();
    
    public Client(MessageBus messageBus, NetworkEndpoint clientEndpoint, List<NetworkEndpoint> serverEndpoints) {
        this.messageBus = messageBus;
        this.clientEndpoint = clientEndpoint;
        this.serverEndpoints = serverEndpoints;
        
        // Register client as a message handler - key insight!
        messageBus.registerHandler(clientEndpoint, this);
    }
    
    public CompletableFuture<byte[]> getObject(String key) {
        String messageId = generateMessageId();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        
        // Track the request
        pendingGets.put(messageId, future);
        
        // Send async message
        GetObjectMessage request = new GetObjectMessage(key, messageId);
        NetworkEndpoint targetServer = selectServer(key);
        messageBus.send(request, clientEndpoint, targetServer);
        
        return future;
    }
    
    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        // Client handles responses just like servers handle requests
        switch (message.getType()) {
            case GET_OBJECT_RESPONSE -> handleGetResponse((GetObjectResponseMessage) message);
            case PUT_OBJECT_RESPONSE -> handlePutResponse((PutObjectResponseMessage) message);
            // ... other response types
        }
    }
    
    private void handleGetResponse(GetObjectResponseMessage response) {
        CompletableFuture<byte[]> future = pendingGets.remove(response.getMessageId());
        if (future != null) {
            if (response.isSuccess()) {
                future.complete(response.getData());
            } else {
                future.completeExceptionally(new RuntimeException(response.getError()));
            }
        }
    }
}
```

#### Benefits of Client-as-Peer Architecture

1. **Uniform Message Handling**: Same patterns for clients and servers
2. **Natural Async Operations**: All operations return futures immediately
3. **Testable Communication**: All messages flow through the same deterministic bus
4. **Scalable Design**: No blocking threads waiting for responses
5. **Fault Tolerance**: Easy to implement retries, timeouts, and circuit breakers

#### Common Pitfalls

```java
// ❌ Wrong - Client trying to be synchronous in async system
public class BadClient {
    public byte[] getObject(String key) {
        // This breaks the async message-passing model
        return asyncGetObject(key).join(); // Blocks without tick progression!
    }
}

// ✅ Correct - Client embraces async nature
public class GoodClient implements MessageBus.MessageHandler {
    public CompletableFuture<byte[]> getObject(String key) {
        // Returns future immediately, completes via message handler
        return sendRequestAndReturnFuture(key);
    }
    
    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        // Handles responses to complete futures
    }
}
```

This client-as-peer pattern is fundamental to building truly asynchronous, message-driven distributed systems where all components participate equally in the message-passing protocol.

This pattern ensures deterministic, scalable, and debuggable distributed systems by controlling exactly when and where asynchronous operations progress.
