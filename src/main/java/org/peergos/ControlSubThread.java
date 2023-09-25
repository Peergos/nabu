package org.peergos;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// docs: https://www.baeldung.com/java-thread-stop
// code: https://github.com/eugenp/tutorials/blob/master/core-java-modules/core-java-concurrency-basic/src/main/java/com/baeldung/concurrent/stopping/ControlSubThread.java
public class ControlSubThread {

    private static final Logger LOG = Logger.getLogger(PeriodicBlockProvider.class.getName());

    private Thread worker;
    private long interval;
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(true);


    public ControlSubThread(long sleepInterval, Callable<Void> callable, String threadName) {
        interval = sleepInterval;
        worker = new Thread(() -> run(callable), threadName);
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        running.set(false);
        worker.interrupt();
    }

    boolean isRunning() {
        return running.get();
    }

    boolean isStopped() {
        return stopped.get();
    }

    public void run(Callable<Void> callable) {
        running.set(true);
        stopped.set(false);
        while (running.get()) {
            try {
                Thread.sleep(interval);
                callable.call();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.log(Level.WARNING, "Thread was interrupted, Failed to complete operation");
            } catch (Throwable throwable) {
                LOG.log(Level.WARNING, throwable.getMessage(), throwable);
            }
        }
        stopped.set(true);
    }
}