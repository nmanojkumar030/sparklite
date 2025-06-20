package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.scheduler.DAGScheduler;
import minispark.scheduler.TaskSchedulerImpl;
import minispark.util.SimulationRunner;
import minispark.worker.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages a Spark cluster for testing purposes, including workers and schedulers.
 */
public class SparkCluster implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SparkCluster.class);
    
    private final MessageBus messageBus;
    private final TaskSchedulerImpl taskScheduler;
    private final DAGScheduler dagScheduler;
    private final List<Worker> workers;
    private final MiniSparkContext sc;
    
    public SparkCluster(MessageBus messageBus, int numWorkers) {
        this.messageBus = messageBus;
        
        // Initialize scheduler
        NetworkEndpoint schedulerEndpoint = new NetworkEndpoint("localhost", 8080);
        taskScheduler = new TaskSchedulerImpl(schedulerEndpoint, messageBus);
        
        // Initialize DAG scheduler
        dagScheduler = new DAGScheduler(taskScheduler);
        
        // Create workers
        workers = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            NetworkEndpoint workerEndpoint = new NetworkEndpoint("localhost", 8090 + i);
            Worker worker = new Worker("worker" + i, workerEndpoint, schedulerEndpoint, 2, messageBus);
            workers.add(worker);
        }
        
        // Initialize Spark context
        sc = new MiniSparkContext(numWorkers);
    }
    
    public void start() {
        // Start all components
        messageBus.start();
        taskScheduler.start();
        workers.forEach(Worker::start);
        SimulationRunner.runUntil(messageBus,
                () -> taskScheduler.getNumWorkers() == workers.size());
        logger.info("Started Spark cluster with {} workers", workers.size());
    }

    public void stop() {
        workers.forEach(Worker::stop);
        taskScheduler.stop();
        messageBus.stop();
        logger.info("Stopped Spark cluster");
    }
    
    @Override
    public void close() {
        stop();
    }
    
    public DAGScheduler getDagScheduler() {
        return dagScheduler;
    }
    
    public MiniSparkContext getSparkContext() {
        return sc;
    }
    
    public List<Worker> getWorkers() {
        return workers;
    }
} 