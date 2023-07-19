package org.apache.rocketmq.store.util;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.store.PutMessageLock;

public class MCSLock implements PutMessageLock {

    AtomicReference<Node> tail;
    ThreadLocal<Node> myNode;

    public MCSLock() {
        myNode = ThreadLocal.withInitial(Node::new);
        tail = new AtomicReference<>();
    }

    @Override
    public void lock() {
        Node qnode = myNode.get();
        qnode.locked = true;
        Node pred = tail.getAndSet(qnode);
        if (pred != null) {
            pred.next = qnode;

            // wait until predecessor gives up the lock
            int cnt = 0;
            while (qnode.locked) {
            }
        }
    }

    @Override
    public void unlock() {
        Node qnode = myNode.get();
        if (qnode.next == null) {
            if (tail.compareAndSet(qnode, null))
                return;

            // wait until predecessor fills in its next field
            while (qnode.next == null) {
            }
        }
        qnode.next.locked = false;
        qnode.next = null;
    }

    class Node {
        volatile boolean locked = true;
        volatile Node next = null;
    }
}