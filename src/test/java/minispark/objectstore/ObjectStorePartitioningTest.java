package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Objects;
import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStorePartitioningTest {
    private MiniSparkContext sc;
    private MessageBus messageBus;
    private Client client;
    private NetworkEndpoint clientEndpoint;
    private List<ServerNode> serverNodes;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        // Create test directories and initialize components
        sc = new MiniSparkContext(2); // 2 partitions for testing
        messageBus = new MessageBus();
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
        
        // Create 10 servers
        int numServers = 10;
        serverNodes = new ArrayList<>();

        for (int i = 0; i < numServers; i++) {
            // Create server node with all components
            ServerNode node = new ServerNode(
                "server-" + i,
                new LocalStorageNode(tempDir.resolve("server" + i).toString()),
                new NetworkEndpoint("localhost", 8081 + i)
            );
            node.server = new Server(node.id, node.storage, messageBus, node.endpoint);
            serverNodes.add(node);
        }

        // Create client with all server endpoints
        client = new Client(messageBus, clientEndpoint, 
            serverNodes.stream()
                .map(node -> node.endpoint)
                .collect(Collectors.toList()));

        // Start message bus
        messageBus.start();
    }

    @Test
    void testSingleObjectInsertion() throws Exception {
        // Create a single customer profile
        CustomerProfile customer = new CustomerProfile(
            "CUST0001",
            "John Doe",
            "john@example.com",
            750
        );

        // Insert the customer data
        String key = "customer-" + customer.getId();
        String jsonData = customer.toJson();
        client.putObject(key, jsonData.getBytes()).get(5, TimeUnit.SECONDS);

        // Verify the data was inserted correctly
        byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
        String retrievedJson = new String(retrievedData);
        CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
        
        assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");

        // Verify data distribution
        System.out.println("\nData Distribution After Single Insert:");
        for (ServerNode node : serverNodes) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    @Test
    void testBatchObjectInsertion() throws Exception {
        // Create multiple customer profiles
        List<CustomerProfile> customers = createCustomerProfiles(5);
        
        // Insert all customers
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            String jsonData = customer.toJson();
            client.putObject(key, jsonData.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Verify all customers were inserted correctly
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
            String retrievedJson = new String(retrievedData);
            CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
            assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");
        }

        // Verify data distribution
        System.out.println("\nData Distribution After Batch Insert:");
        for (ServerNode node : serverNodes) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    @Test
    void testConcurrentObjectInsertion() throws Exception {
        // Create multiple customer profiles
        List<CustomerProfile> customers = createCustomerProfiles(10);
        
        // Insert all customers concurrently
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            String jsonData = customer.toJson();
            futures.add(client.putObject(key, jsonData.getBytes()));
        }

        // Wait for all insertions to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(10, TimeUnit.SECONDS);

        // Verify all customers were inserted correctly
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
            String retrievedJson = new String(retrievedData);
            CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
            assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");
        }

        // Verify data distribution
        System.out.println("\nData Distribution After Concurrent Insert:");
        for (ServerNode node : serverNodes) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    @Test
    void testDataPartitioningAcrossServers() throws Exception {
        // Create customer profile data
        List<CustomerProfile> customers = createCustomerProfiles(20);
        
        // Write customer data to ObjectStore
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            client.putObject(key, customer.toJson().getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Create RDD to read the data
        ObjectStoreRDD rdd = new ObjectStoreRDD(sc, client, "customer-", 2);

        // Collect and verify data
        List<byte[]> result = rdd.collect();
        assertEquals(20, result.size());

        // Convert byte arrays back to CustomerProfile objects
        List<CustomerProfile> retrievedCustomers = result.stream()
            .map(bytes -> CustomerProfile.fromJson(new String(bytes)))
            .collect(Collectors.toList());

        // Verify data consistency
        assertEquals(customers.size(), retrievedCustomers.size());
        assertEquals(new HashSet<>(customers), new HashSet<>(retrievedCustomers));

        // Print data distribution across servers
        System.out.println("\nData Distribution Across Servers:");
        for (ServerNode node : serverNodes) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
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

    private static class CustomerProfile implements Serializable {
        private static final long serialVersionUID = 1L;
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
            // Simple JSON parsing - in a real implementation, use a proper JSON library
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
} 