package com.phonepe.phonepedemo.queue;

public class LinkedListQueue<T>
{
    int capacity=1000;
    float ttlSeconds=10;
    boolean ttlRequired=false;
    private Node front, rear;
    int consumerCount=1;
    private int queueSize; // queue size

    //linked list node
    private class Node {
        T data;
        long timestamp;
        Node next;
        int readCount;
    }

    //default constructor - initially front & rear are null; size=0; queue is empty
    public LinkedListQueue(int capacity, float ttlSeconds, int consumerCount)
    {
        front = null;
        rear = null;
        queueSize = 0;
        this.capacity = capacity;
        this.ttlSeconds=ttlSeconds;
        this.ttlRequired=true;
        this.consumerCount=consumerCount;
    }

    public LinkedListQueue()
    {
        front = null;
        rear = null;
        queueSize = 0;
    }


    //check if the queue is empty
    public boolean isEmpty()
    {
        validateQueue();
        return (queueSize == 0);
    }

    //check if the queue is empty
    public boolean isFull()
    {
        validateQueue();
        return (queueSize == capacity);
    }

    //only check the data at the start of the queue.
    public T poll()
    {
        validateQueue();
        if (isEmpty()){
            System.out.println("Queue is empty");
            return null;
        }
        T data = front.data;
        System.out.println("Element " + data+ " is polled");
        return data;
    }

    //Remove item from the front of the queue.
    public T dequeue()
    {
        validateQueue();
        if (isEmpty()){
            System.out.println("Queue is empty");
            return null;
        }
        T data = front.data;
        front = front.next;
        if (isEmpty())
        {
            rear = null;
        }
        queueSize--;
        System.out.println("Element " + data+ " removed from the queue");
        return data;
    }

    //Add data at the rear of the queue.
    public boolean enqueue(T data)
    {
        validateQueue();
        if (queueSize == capacity){
            System.out.println("Queue is full");
            return false;
        }
        Node oldRear = rear;
        rear = new Node();
        rear.data = data;
        rear.timestamp = System.currentTimeMillis();
        rear.next = null;
        if (isEmpty())
        {
            front = rear;
        }
        else  {
            oldRear.next = rear;
        }
        queueSize++;
        System.out.println("Element " + data+ " added to the queue");
        return true;
    }

    public void validateQueue(){
        if (queueSize == 0){
            return;
        }
        while(front!=null && queueSize != 0){
            long timestamp = front.timestamp;
            long currentTimestamp=System.currentTimeMillis();
            float sec = (currentTimestamp - timestamp) / 1000F;
            if (ttlSeconds < sec){
                front = front.next;
                queueSize--;
            }else{
                return;
            }
        }
    }
}
