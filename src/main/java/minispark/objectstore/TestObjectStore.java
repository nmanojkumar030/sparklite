package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestObjectStore {
    public static void main(String[] args) {
        try {
            // Initialize LocalStorageNode
            LocalStorageNode storageNode = new LocalStorageNode("/tmp/storage");
            
            // Initialize MessageBus
            MessageBus messageBus = new MessageBus();
            messageBus.start();
            
            // Define endpoints
            NetworkEndpoint clientEndpoint = new NetworkEndpoint("localhost", 8080);
            NetworkEndpoint serverEndpoint = new NetworkEndpoint("localhost", 8081);
            
            // Initialize Server
            Server server = new Server(storageNode, messageBus, serverEndpoint);
            
            // Initialize Client with list of server endpoints
            Client client = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));
            
            // Test PUT operation
            String key = "testKey";
            byte[] data = "Hello, World!".getBytes();
            client.putObject(key, data).get(5, TimeUnit.SECONDS);
            
            // Test GET operation
            byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
            System.out.println("Retrieved data: " + new String(retrievedData));
            
            // Test DELETE operation
            client.deleteObject(key).get(5, TimeUnit.SECONDS);
            
            // Test LIST operation
            List<String> objects = client.listObjects().get(5, TimeUnit.SECONDS);
            System.out.println("Objects: " + objects);
            
            // Stop the MessageBus
            messageBus.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} 