package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.core.Partition;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.scheduler.TaskSchedulerImpl;
import minispark.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Objects;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoreWorkerTest {
    private MessageBus messageBus;
    private NetworkEndpoint schedulerEndpoint;
    private NetworkEndpoint clientEndpoint;
    private TaskSchedulerImpl taskScheduler;
    private Client client;
    private List<ServerNode> serverNodes;
    private List<Worker> workers;
    private MiniSparkContext sc;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        // Initialize message bus
        messageBus = new MessageBus();
        
        // Initialize endpoints
        schedulerEndpoint = new NetworkEndpoint("localhost", 8080);
        clientEndpoint = new NetworkEndpoint("localhost", 8081);
        
        // Initialize scheduler
        taskScheduler = new TaskSchedulerImpl(schedulerEndpoint, messageBus);
        
        // Create server nodes
        int numServers = 3;
        serverNodes = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            ServerNode node = new ServerNode(
                "server-" + i,
                new LocalStorageNode(tempDir.resolve("server" + i).toString()),
                new NetworkEndpoint("localhost", 8082 + i)
            );
            node.server = new Server(node.id, node.storage, messageBus, node.endpoint);
            serverNodes.add(node);
        }

        // Create client with all server endpoints
        client = new Client(messageBus, clientEndpoint, 
            serverNodes.stream()
                .map(node -> node.endpoint)
                .collect(Collectors.toList()));

        // Create workers
        int numWorkers = 2;
        workers = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            NetworkEndpoint workerEndpoint = new NetworkEndpoint("localhost", 8090 + i);
            Worker worker = new Worker("worker" + i, workerEndpoint, schedulerEndpoint, 2, messageBus);
            workers.add(worker);
        }

        // Initialize Spark context
        sc = new MiniSparkContext(numWorkers);

        // Start all components
        messageBus.start();
        taskScheduler.start();
        workers.forEach(Worker::start);

        // Wait for workers to register
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterEach
    void tearDown() {
        workers.forEach(Worker::stop);
        taskScheduler.stop();
        messageBus.stop();
    }

    @Test
    void testEndToEndPartitioningAndExecution() throws Exception {
        // Create customer profile data
        List<CustomerProfile> customers = createCustomerProfiles(10);
        
        // Write customer data to ObjectStore
        System.out.println("\nInserting customer data:");
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            String jsonData = customer.toJson();
            client.putObject(key, jsonData.getBytes()).get(5, TimeUnit.SECONDS);
            System.out.printf("Inserted customer %s to server %s%n", 
                customer.getId(), 
                getServerForKey(key).id);
        }

        // Create RDD with multiple partitions
        ObjectStoreRDD rdd = new ObjectStoreRDD(sc, client, "customer-", 2);

        // Create a task that processes customer data
        List<CustomerProcessingTask> tasks = new ArrayList<>();
        for (Partition partition : rdd.getPartitions()) {
            tasks.add(new CustomerProcessingTask(
                tasks.size(),
                0, // stageId
                partition.getPartitionId(),
                rdd
            ));
        }

        // Submit tasks to scheduler
        System.out.println("\nSubmitting tasks to workers:");
        List<CompletableFuture<List<CustomerProfile>>> futures = taskScheduler.submitTasks(tasks, 2);

        // Wait for all tasks to complete
        List<CustomerProfile> processedCustomers = new ArrayList<>();
        for (CompletableFuture<List<CustomerProfile>> future : futures) {
            processedCustomers.addAll(future.get(10, TimeUnit.SECONDS));
        }

        // Verify results
        assertEquals(customers.size(), processedCustomers.size());
        assertEquals(new HashSet<>(customers), new HashSet<>(processedCustomers));

        // Print execution details
        System.out.println("\nExecution Summary:");
        System.out.printf("Total customers: %d%n", customers.size());
        System.out.printf("Number of partitions: %d%n", rdd.getPartitions().length);
        System.out.printf("Number of workers: %d%n", workers.size());
        
        // Print data distribution
        System.out.println("\nData Distribution Across Servers:");
        for (ServerNode node : serverNodes) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    private ServerNode getServerForKey(String key) {
        NetworkEndpoint targetServer = client.getTargetServer(key);
        return serverNodes.stream()
            .filter(node -> node.endpoint.equals(targetServer))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No server found for key: " + key));
    }

    private List<CustomerProfile> createCustomerProfiles(int count) {
        List<CustomerProfile> customers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            customers.add(new CustomerProfile(
                "CUST" + String.format("%04d", i),
                "Customer " + i,
                "customer" + i + "@example.com",
                new Random().nextInt(1000000)
            ));
        }
        return customers;
    }

    private int countObjectsForServer(LocalStorageNode storageNode) {
        try {
            return storageNode.listObjects("").size();
        } catch (Exception e) {
            return 0;
        }
    }

    private static class ServerNode {
        final String id;
        final LocalStorageNode storage;
        final NetworkEndpoint endpoint;
        Server server;

        ServerNode(String id, LocalStorageNode storage, NetworkEndpoint endpoint) {
            this.id = id;
            this.storage = storage;
            this.endpoint = endpoint;
        }
    }

    private static class CustomerProfile {
        private final String id;
        private final String name;
        private final String email;
        private final int creditScore;

        public CustomerProfile(String id, String name, String email, int creditScore) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.creditScore = creditScore;
        }

        public String getId() { return id; }
        public String getName() { return name; }
        public String getEmail() { return email; }
        public int getCreditScore() { return creditScore; }

        public String toJson() {
            return String.format(
                "{\"id\":\"%s\",\"name\":\"%s\",\"email\":\"%s\",\"creditScore\":%d}",
                id, name, email, creditScore
            );
        }

        public static CustomerProfile fromJson(String json) {
            String[] parts = json.substring(1, json.length() - 1).split(",");
            String id = parts[0].split(":")[1].replace("\"", "");
            String name = parts[1].split(":")[1].replace("\"", "");
            String email = parts[2].split(":")[1].replace("\"", "");
            int creditScore = Integer.parseInt(parts[3].split(":")[1]);
            return new CustomerProfile(id, name, email, creditScore);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomerProfile that = (CustomerProfile) o;
            return creditScore == that.creditScore &&
                   Objects.equals(id, that.id) &&
                   Objects.equals(name, that.name) &&
                   Objects.equals(email, that.email);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, email, creditScore);
        }
    }

    private static class CustomerProcessingTask extends minispark.core.Task<byte[], List<CustomerProfile>> {
        private final ObjectStoreRDD rdd;
        private static final Logger logger = LoggerFactory.getLogger(CustomerProcessingTask.class);

        CustomerProcessingTask(int taskId, int stageId, int partitionId, ObjectStoreRDD rdd) {
            super(taskId, stageId, partitionId);
            this.rdd = rdd;
        }

        @Override
        public List<CustomerProfile> execute(Partition<byte[]> partition) {
            // Create a new ObjectStorePartition with the correct base key
            ObjectStoreRDD.ObjectStorePartition objectStorePartition = new ObjectStoreRDD.ObjectStorePartition(
                partition.getPartitionId(),
                "customer-" // Use the same base key as in the test
            );
            
            logger.debug("Processing partition {} with base key {}", 
                objectStorePartition.getPartitionId(), 
                objectStorePartition.getBaseKey());
            
            List<CustomerProfile> processedCustomers = new ArrayList<>();
            Iterator<byte[]> iter = rdd.compute(objectStorePartition);
            
            while (iter.hasNext()) {
                CustomerProfile customer = CustomerProfile.fromJson(new String(iter.next()));
                processedCustomers.add(customer);
                logger.debug("Processed customer {} in partition {}", 
                    customer.getId(), 
                    objectStorePartition.getPartitionId());
            }
            
            logger.debug("Partition {} processed {} customers", 
                objectStorePartition.getPartitionId(), 
                processedCustomers.size());
            
            return processedCustomers;
        }
    }
} 