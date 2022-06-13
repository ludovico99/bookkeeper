package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;




public class LedgerCreateOpTests {

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

    @RunWith(value = Parameterized.class)
    public static class LedgerCreateOpInitiateTest extends BookKeeperClusterTestCase {

        private Object expectedException;
        private Integer ensembleSize;
        private Integer writeQuorumSize;
        private Integer ackQuorumSize;
        private SyncCallbackUtils.SyncCreateCallback cb;


        public LedgerCreateOpInitiateTest(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, ParamType cb, Object expectedException) {
            super(3);
            configureInitiate(ensembleSize, writeQuorumSize, ackQuorumSize, cb, expectedException);
        }

        private void configureInitiate(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, ParamType cb, Object expectedException) {

            this.expectedException = expectedException;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;

            switch (cb) {
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
                    //ensembleSize,       writeQuorumSize,     ackQuorumSize        CB                                 Exception
                    {2, 2, 2, ParamType.VALID_INSTANCE, BKException.Code.OK},
                    {3, 2, 2, ParamType.VALID_INSTANCE, BKException.Code.OK},
                    {3, 1, 1, ParamType.VALID_INSTANCE, BKException.Code.OK},
                    {1, 2, 2, ParamType.VALID_INSTANCE, BKException.Code.ZKException},
                    {3, 4, 2, ParamType.VALID_INSTANCE, BKException.Code.ZKException},
                    {11, 10, 2, ParamType.VALID_INSTANCE, BKException.Code.NotEnoughBookiesException},
                    {3, 4, 5, ParamType.VALID_INSTANCE, BKException.Code.ZKException},
                    {3, 2, 3, ParamType.VALID_INSTANCE, BKException.Code.ZKException},
                    {3, 4, 2, ParamType.VALID_INSTANCE, BKException.Code.ZKException},
                    {10, 5, 2, ParamType.VALID_INSTANCE, BKException.Code.NotEnoughBookiesException},
                    {null, 1, 1, ParamType.VALID_INSTANCE, new NullPointerException()},
                    {3, null, 1, ParamType.VALID_INSTANCE, new NullPointerException()},
                    {3, 1, null, ParamType.VALID_INSTANCE, new NullPointerException()}


            });
        }


        @Test
        public void Test_Initiate() {

            try {
                BookKeeper bookkeeper = this.bkc;
                Counter counter = new Counter();
                LedgerCreateOp ledgerCreateOp = new LedgerCreateOp(bookkeeper, ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                        "pwd".getBytes(StandardCharsets.UTF_8), this.cb, counter, null, EnumSet.allOf(WriteFlag.class), bookkeeper.getClientCtx().getClientStats());

                if ((int) this.expectedException == BKException.Code.ZKException) {
                    // Non va i busy waiting poichè mi aspetto di non avere risposta !!!
                    ledgerCreateOp.initiate();
                    verifyNoInteractions(this.cb);
                } else {
                    counter.inc();

                    ledgerCreateOp.initiate();

                    counter.wait(0);

                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class), isA(Object.class));
                    Assert.assertEquals(this.expectedException, argument.getValue());
                }


            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertEquals("Exception that i expect is raised", this.expectedException.getClass(), e.getClass());
            }

        }


        @RunWith(value = Parameterized.class)
        public static class LedgerCreateOpInitiateAdvTest extends BookKeeperClusterTestCase {

            private Object expectedException;
            private Integer ensembleSize;
            private Integer writeQuorumSize;
            private Integer ackQuorumSize;
            private Long ledgerId;
            private SyncCallbackUtils.SyncCreateCallback cb;

            public LedgerCreateOpInitiateAdvTest(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, Long ledgerId, ParamType cb, Object expectedException) {
                super(3);
                configureInitiateAdv(ensembleSize, writeQuorumSize, ackQuorumSize, ledgerId, cb, expectedException);
            }


            private void configureInitiateAdv(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, Long ledgerId, ParamType cb, Object expectedException) {

                this.expectedException = expectedException;
                this.ensembleSize = ensembleSize;
                this.writeQuorumSize = writeQuorumSize;
                this.ackQuorumSize = ackQuorumSize;
                this.ledgerId = ledgerId;

                switch (cb) {
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
                        //ensembleSize,       writeQuorumSize,     ackQuorumSize    ledgerId             CB                                 Exception
                        {2, 2, 2, 0L, ParamType.VALID_INSTANCE, BKException.Code.OK},
                        {3, 2, 2, 0L, ParamType.VALID_INSTANCE, BKException.Code.OK},
                        {3, 1, 1, 0L, ParamType.VALID_INSTANCE, BKException.Code.OK},
                        {1, 2, 2, 0L, ParamType.VALID_INSTANCE, true},
                        {3, 4, 2, 0L, ParamType.VALID_INSTANCE, true},
                        {11, 10, 2, 0L, ParamType.VALID_INSTANCE, BKException.Code.NotEnoughBookiesException},
                        {3, 4, 5, 0L, ParamType.VALID_INSTANCE, true},
                        {3, 2, 3, 0L, ParamType.VALID_INSTANCE, true},
                        {3, 4, 2, 0L, ParamType.VALID_INSTANCE, true},
                        {10, 5, 2, 0L, ParamType.VALID_INSTANCE, BKException.Code.NotEnoughBookiesException},
                        {null, 1, 1, 0L, ParamType.VALID_INSTANCE, true},
                        {3, null, 1, 0L, ParamType.VALID_INSTANCE, true},
                        {3, 2, null, 0L, ParamType.VALID_INSTANCE, true},
                        {3, 2, 1, -1L, ParamType.VALID_INSTANCE, BKException.Code.OK},
                        {3, 2, 1, -2L, ParamType.VALID_INSTANCE, true},
                        {3, 2, 1, null, ParamType.VALID_INSTANCE, true}


                });
            }


            @Test
            public void Test_InitiateAdv() {

                try {
                    BookKeeper bookkeeper = this.bkc;
                    LedgerCreateOpTests.Counter counter = new LedgerCreateOpTests.Counter();
                    LedgerCreateOp ledgerCreateOp = new LedgerCreateOp(bookkeeper, ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                            "pwd".getBytes(StandardCharsets.UTF_8), this.cb, counter, null, EnumSet.allOf(WriteFlag.class), bookkeeper.getClientCtx().getClientStats());

                    if ((int) this.expectedException == BKException.Code.ZKException) {
                        // Non va i busy waiting poichè mi aspetto di non avere risposta !!!
                        ledgerCreateOp.initiateAdv(this.ledgerId);
                        verifyNoInteractions(this.cb);

                    } else {
                        counter.inc();

                        ledgerCreateOp.initiateAdv(this.ledgerId);

                        counter.wait(0);

                        ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                        verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class), isA(Object.class));
                        Assert.assertEquals(this.expectedException, argument.getValue());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue("Exception that i expect is raised", (boolean) this.expectedException);

                }

            }
        }

        private static SyncCallbackUtils.SyncCreateCallback createCallback() {

            return spy(new SyncCallbackUtils.SyncCreateCallback(new CompletableFuture<>()) {

                @Override
                public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                    Counter counter = (Counter) ctx;
                    counter.dec();
                    System.out.println("Create complete: rc = " + rc + " counter value: " + counter.i);
                }

            });
        }
    }
}


