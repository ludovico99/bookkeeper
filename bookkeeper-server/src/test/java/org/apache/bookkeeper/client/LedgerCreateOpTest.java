package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
@Ignore
public class LedgerCreateOpTest extends BookKeeperClusterTestCase {

    private Object expectedException;
    private Integer ensembleSize;
    private Integer writeQuorumSize;
    private Integer ackQuorumSize;
    private AsyncCallback.CreateCallback cb;

    public LedgerCreateOpTest(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize,ParamType cb, Object expectedException) {
        super(3);
        configureInitiate(ensembleSize, writeQuorumSize, ackQuorumSize, cb, expectedException);
    }

    private void configureInitiate(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, ParamType cb, Object expectedException) {

        this.expectedException = expectedException;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

        switch (cb){
            case VALID_INSTANCE:
                this.cb = createCallback();
                break;
            case NULL_INSTANCE:
                this.cb = null;
                break;
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //Totale bookies = 3
                //ensembleSize,       writeQuorumSize,     ackQuorumSize     CB                        Exception
                {1,                             2,                  2, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {3,                             4,                  2, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {11,                            10,                 2, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {3,                             4,                  5, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {3,                             2,                  3, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {3,                             4,                  2, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {10,                            5,                  2, ParamType.VALID_INSTANCE,             BKException.Code.ZKException},
                {2,                             2,                  2, ParamType.VALID_INSTANCE,             BKException.Code.OK},
                {3,                             2,                  2, ParamType.VALID_INSTANCE,             BKException.Code.OK},
                {3,                             1,                  1, ParamType.VALID_INSTANCE,             BKException.Code.OK},
                {null,                          1,                  1, ParamType.VALID_INSTANCE,             new NullPointerException()},
                {3,                             null,               1, ParamType.VALID_INSTANCE,             new NullPointerException()},
                {3,                             1,                  null , ParamType.VALID_INSTANCE,         new NullPointerException()}


        });
    }



    @Test
    public void Test_Initiate() {

         try{
                Counter counter = new Counter();

                BookKeeper bookkeeper = this.bkc;

                counter.inc();

//                bkc.asyncCreateLedger(this.writeEnsembleSize,this.writeQuorumSize,this.ackQuorumSize, BookKeeper.DigestType.CRC32,
//                      "pwd".getBytes(StandardCharsets.UTF_8),this.cb,counter,null);

                LedgerCreateOp ledgerCreateOp = new LedgerCreateOp(bookkeeper, ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                        "pwd".getBytes(StandardCharsets.UTF_8), this.cb, counter, null, EnumSet.allOf(WriteFlag.class), bookkeeper.getClientCtx().getClientStats());
                ledgerCreateOp.initiate();
                counter.wait(0);

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class),isA(Object.class));
                Assert.assertEquals(this.expectedException, argument.getValue());

                } catch (Exception e) {
                e.printStackTrace();
                Assert.assertEquals("Exception that i expect is raised", this.expectedException.getClass(), e.getClass());
                //Assert.assertTrue("Exception raised as i expected", (Boolean) this.expectedException);
            }

    }

    private AsyncCallback.CreateCallback createCallback(){

        return spy(new AsyncCallback.CreateCallback (){

            @Override
            public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("Create complete: rc = " + rc + "counter value: " + counter.i);
            }

        });
    }

    private static class Counter {
        int i;
        int total;

        synchronized void inc() {
            i++;
            total++;
        }

        synchronized void dec() {
            i--;
            notifyAll();
        }

        synchronized void wait(int limit) throws InterruptedException {
            while (i > limit) {
                wait();
            }
        }

        synchronized int total() {
            return total;
        }
    }


}


