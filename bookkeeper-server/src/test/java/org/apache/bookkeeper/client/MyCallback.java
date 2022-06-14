package org.apache.bookkeeper.client;

import org.apache.bookkeeper.util.Counter;

public class MyCallback implements AsyncCallback.CreateCallback {

    @Override
    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
        Counter counter = (Counter) ctx;
        counter.dec();
        System.out.println("Create complete: rc = " + rc + " counter value: " + counter.i);
    }
}

