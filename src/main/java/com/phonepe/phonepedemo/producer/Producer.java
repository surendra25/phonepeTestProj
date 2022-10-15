package com.phonepe.phonepedemo.producer;

import com.phonepe.phonepedemo.queue.QueueManager;
import org.json.JSONObject;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Producer implements Runnable{

    final QueueManager queueManager;
    List<JSONObject> messages;
    ReentrantLock lock;
    Condition notEmptySignal, notFullSignal;

    public Producer(QueueManager queueManager, List<JSONObject> messages,
                    ReentrantLock lock, Condition notEmptySignal, Condition notFullSignal){
        this.queueManager=queueManager;
        this.messages=messages;
        this.lock=lock;
        this.notFullSignal= notFullSignal;
        this.notEmptySignal=notEmptySignal;
    }
    @Override public void run() {
        System.out.println("Producer run called");
        lock.lock();
        try{
            for (JSONObject message:messages){
                int sleep=0;
                if (message.has("sleep"))
                    sleep=message.getInt("sleep");
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    System.out.println("Exception in thread sleep");
                }
                if (queueManager.queueFull() && !Thread.currentThread().isInterrupted()){
                    try {
                        notEmptySignal.await(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                queueManager.produceMessage(message);
                notFullSignal.signalAll();
            }
        }finally {
            lock.unlock();
        }

    }
}
