package com.phonepe.phonepedemo.queue;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class QueueManager {
    private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);
    private static QueueManager queueManager=null;
    private static LinkedListQueue<JSONObject> linkedListQueue=null;
    private static LinkedListQueue<JSONObject> sideLineQueue=null;
    private final int maxRetry=3;
    private QueueManager(){}

    public synchronized static QueueManager getInstance(int capacity, float ttlSeconds,
                                                        int consumerCount){
        if (queueManager == null) {
            queueManager = new QueueManager();
            linkedListQueue=new LinkedListQueue<JSONObject>(capacity, ttlSeconds, consumerCount);
            sideLineQueue=new LinkedListQueue<JSONObject>();
        }
        return queueManager;
    }

    public synchronized boolean produceMessage(JSONObject message) {
        boolean response = linkedListQueue.enqueue(message);
        if (response) {
            return true;
        } else {
            int retry = 0;
            while (retry < maxRetry) {
                response = linkedListQueue.enqueue(message);
                if (response) {
                    return true;
                }
                retry++;
            }
        }
        System.out.println("Message is not processed moved to sideline-"+message);
        sideLineQueue.enqueue(message);
        return false;
    }

    public synchronized boolean queueEmpty(){
        return linkedListQueue.isEmpty();
    }

    public synchronized boolean queueFull(){
        return linkedListQueue.isFull();
    }

    public synchronized JSONObject consumeMessage(Map<String, String> criteria) {
        JSONObject data = linkedListQueue.poll();
        if (data==null){
            System.out.println("Queue is empty, cant consume the message");
            return null;
        }
        if (!data.has("criteria") && (criteria == null || criteria.isEmpty())){
            return linkedListQueue.dequeue();
        }else if(criteria==null || criteria.isEmpty()){
            return null;
        }
        boolean data_found=false;
        for (Map.Entry<String, String> entry:criteria.entrySet()){
            if (data.has(entry.getKey()) && data.getString(entry.getKey()).equalsIgnoreCase(entry.getValue())){
                data_found = true;
            }
        }
        if (data_found){
            return linkedListQueue.dequeue();
        }
        System.out.println("Message does not match to the criteria-"+criteria);
        return null;
    }
}
