# SparkLite

SparkLite is a lightweight distributed computing framework inspired by Apache Spark. It implements core distributed computing concepts in a simplified manner, making it ideal for learning and understanding distributed systems.

## Features

- **Distributed Task Execution**: Execute tasks across multiple workers in a distributed environment
- **Message-Based Communication**: Reliable communication between scheduler and workers using a message bus
- **Worker Management**: Dynamic worker registration and health monitoring
- **Fault Tolerance**: Handle task failures and worker node failures gracefully
- **Future-Based Results**: Asynchronous task execution with CompletableFuture results

## Architecture

The system consists of several key components:

1. **TaskScheduler**: Manages task distribution and worker coordination
2. **Worker**: Executes tasks and reports results back to the scheduler
3. **MessageBus**: Handles communication between components
4. **Task**: Base class for defining distributed computations

## Getting Started

### Prerequisites

- Java 21 or higher
- Gradle 8.x

### Building the Project

```bash
./gradlew build
```

### Running Tests

```bash
./gradlew test
```

## Usage Example

Here's a simple example of using SparkLite to execute distributed tasks:

```java
// Create message bus and network endpoints
MessageBus messageBus = new MessageBus();
NetworkEndpoint schedulerEndpoint = new NetworkEndpoint("localhost", 8080);

// Create and start task scheduler
TaskSchedulerImpl taskScheduler = new TaskSchedulerImpl(schedulerEndpoint, messageBus);
taskScheduler.start();

// Create and start workers
Worker worker1 = new Worker("worker1", new NetworkEndpoint("localhost", 8081), 
    schedulerEndpoint, 2, messageBus);
worker1.start();

// Define and submit tasks
TestTask task = new TestTask(1, 1, 0, 5);
List<CompletableFuture<Integer>> futures = taskScheduler.submitTasks(
    Arrays.asList(task), 1);

// Get results
Integer result = futures.get(0).get(5, TimeUnit.SECONDS);
```

## Project Structure

```
src/
├── main/java/minispark/
│   ├── core/           # Core abstractions (Task, Partition)
│   ├── messages/       # Message definitions
│   ├── network/        # Network communication
│   ├── scheduler/      # Task scheduling
│   └── worker/         # Worker implementation
└── test/java/
    └── minispark/      # Test cases
```

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by Apache Spark's architecture
- Built for educational purposes to demonstrate distributed computing concepts 