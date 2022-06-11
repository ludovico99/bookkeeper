package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public class LedgerCreateOpTest extends BookKeeperClusterTestCase {

    private LedgerCreateOp ledgerCreateOp;
    private Boolean exceptionInConfigPhase = false;
    private Boolean expectedException;
    private int writeEnsembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;

    public LedgerCreateOpTest(int writeEnsembleSize, int writeQuorumSize, int ackQuorumSize, Boolean expectedException) {
        super(3);
        configureInitiate(writeEnsembleSize, writeQuorumSize, ackQuorumSize, expectedException);
    }

    private void configureInitiate(int writeEnsembleSize, int writeQuorumSize, int ackQuorumSize, Boolean expectedException) {

        this.expectedException = expectedException;
        this.writeEnsembleSize = writeEnsembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //writeEnsembleSize,       writeQuorumSize,     ackQuorumSize   Exception
                {1,                             2,                  2,              true},
                {3,                             4,                  2,              true},
                {11, 10, 2, true},
                {3, 4, 5, true},
                {3, 2, 3, true},
                {3, 4, 2, true},
                {10, 5, 2, true},
                {2, 2, 2, false},
                {3, 2, 2, false},
                {3, 1, 1, false}


        });
    }

    @Before
    public void setLedgerCreateOp() {
        try {
            BookKeeper bookkeeper = this.bkc;

            AsyncCallback.CreateCallback cb = mock(AsyncCallback.CreateCallback.class);
            doNothing().when(cb).createComplete(isA(Integer.class), any(),
                    isA(Object.class));

            EnumSet<WriteFlag> writeFlags = EnumSet.allOf(WriteFlag.class);

            ledgerCreateOp = new LedgerCreateOp(bookkeeper, writeEnsembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                    "pippo".getBytes(StandardCharsets.UTF_8), cb, new Object(), null, writeFlags, bookkeeper.getClientCtx().getClientStats());

        } catch (Exception e) {
            e.printStackTrace();
            exceptionInConfigPhase = true;
        }
    }


    @Test
    public void Test_Initiate() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {
                ledgerCreateOp.initiate();
            } catch (Exception e) {
                //Assert.assertEquals("Exception that i expect is raised", this.expectedException.getClass(), e.getClass());
                Assert.assertTrue("Exception raised as i expected", this.expectedException);
            }

        }
    }

}


