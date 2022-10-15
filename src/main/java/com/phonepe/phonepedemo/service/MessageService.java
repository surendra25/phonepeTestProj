package com.phonepe.phonepedemo.service;

import com.phonepe.phonepedemo.consumer.Consumer;
import com.phonepe.phonepedemo.producer.Producer;
import com.phonepe.phonepedemo.queue.QueueManager;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class MessageService {

    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);
    final ReentrantLock lock=new ReentrantLock();
    final Condition notFullSignal= lock.newCondition();
    final Condition notEmptySignal= lock.newCondition();
    final QueueManager queueManager;

    public MessageService() throws InterruptedException {
        queueManager=QueueManager.getInstance(2, 2f, 3);
        Map<String, String> criteria1= new HashMap<>();
        criteria1.put("criteria1", "val1");
        Map<String, String> criteria2= new HashMap<>();
        criteria2.put("criteria1", "val2");

        Consumer consumer = new Consumer(queueManager, "consumer0", null, lock, notEmptySignal, notFullSignal);
        Consumer consumer1 = new Consumer(queueManager, "consumer1", criteria1, lock, notEmptySignal, notFullSignal);
        Consumer consumer2 = new Consumer(queueManager, "consumer2", criteria2, lock, notEmptySignal, notFullSignal);
        Thread t1=new Thread(consumer);
        Thread t2=new Thread(consumer1);
        Thread t3=new Thread(consumer2);
        t1.start();
        t2.start();
        t3.start();

    }

    public void produceMessage(String message) {
        List<JSONObject> list=new ArrayList<>();
        JSONArray arr=new JSONArray(message);
        for (int i=0; i<arr.length(); i++){
            list.add(arr.getJSONObject(i));
        }
        Producer producer=new Producer(queueManager, list, lock, notEmptySignal, notFullSignal);
        new Thread(producer).start();
    }
}
