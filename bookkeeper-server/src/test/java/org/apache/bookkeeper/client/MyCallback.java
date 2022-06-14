package org.apache.bookkeeper.client;

public interface MyCallback extends AsyncCallback.CreateCallback{

    void createComplete(int rc, LedgerHandle lh, Object ctx);
}
