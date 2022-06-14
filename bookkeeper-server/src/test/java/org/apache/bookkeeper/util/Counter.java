package org.apache.bookkeeper.util;

public class Counter {
    public int i;
    public int total;

    synchronized public void inc() {
        i++;
        total++;
    }

    synchronized public void dec() {
        i--;
        notifyAll();
    }

    synchronized public void wait(int limit) throws InterruptedException {
        while (i > limit) {
            wait();
        }
    }

    synchronized public int total() {
        return total;
    }
}

