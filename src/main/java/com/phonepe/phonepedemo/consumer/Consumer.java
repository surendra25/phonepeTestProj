package com.phonepe.phonepedemo.consumer;

import com.phonepe.phonepedemo.queue.QueueManager;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Consumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    final QueueManager queueManager;
    Map<String, String> criteria;
    String name;
    ReentrantLock lock;
    Condition notEmptySignal, notFullSignal;

    public Consumer(QueueManager queueManager, String name, Map<String, String> criteria,
                    ReentrantLock lock, Condition notEmptySignal, Condition notFullSignal) {
        this.queueManager = queueManager;
        this.criteria = criteria;
        this.name = name;
        this.lock=lock;
        this.notFullSignal= notFullSignal;
        this.notEmptySignal=notEmptySignal;
    }

    @Override public void run() {
        System.out.println("Consumer run called-"+name);
        while (true) {
            lock.lock();
            try {
                if (queueManager.queueEmpty() && !Thread.currentThread().isInterrupted()) {
                    try {
                        notFullSignal.await(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }else{
                    JSONObject message=queueManager.consumeMessage(criteria);
                    if (message == null) {
                        try {
                            notFullSignal.await(1000, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }else{
                        System.out.println(name+" consumer consumed message-"+message);
                    }
                    notEmptySignal.signalAll();
                }
            }finally {
                lock.unlock();
            }
        }
    }
}
