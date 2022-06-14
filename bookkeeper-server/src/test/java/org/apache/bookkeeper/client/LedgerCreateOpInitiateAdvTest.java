package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class LedgerCreateOpInitiateAdvTest extends BookKeeperClusterTestCase {

    private Object expectedException;
    private Integer ensembleSize;
    private Integer writeQuorumSize;
    private Integer ackQuorumSize;
    private Long ledgerId;
    private AsyncCallback.CreateCallback cb;
    private ClientConfType clientConfType;

    public LedgerCreateOpInitiateAdvTest(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, Long ledgerId, ParamType cb, ClientConfType clientConfType, Object expectedException) {
        super(3);
        configureInitiateAdv(ensembleSize, writeQuorumSize, ackQuorumSize, ledgerId, cb,clientConfType, expectedException);
    }


    private void configureInitiateAdv(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, Long ledgerId, ParamType cb, ClientConfType clientConfType, Object expectedException) {
        this.clientConfType = clientConfType;
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
                //ensembleSize,       writeQuorumSize,     ackQuorumSize,    ledgerId,             CB,                         Client conf,      Exception
                {null,                  1,                  1,              0L,                 ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, true}, //new NullPointerException
                {3,                     null,               1,              0L,                 ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, true}, //new NullPointerException
                {3,                     2,                  null,           0L,                 ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, true}, //new NullPointerException
                {3,                     2,                  1,              null,               ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, true}, //new NullPointerException

                {1,                     2,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {1,                     2,                  3,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {3,                     2,                  3,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {3,                     2,                  1,             -2L,                ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException

                {3,                     2,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.OK},
                {3,                     2,                  2,             -1L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF,BKException.Code.OK},

                {10,                    5,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {5,                     6,                  7,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {5,                     6,                  7,             -2L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException

                {3,                     2,                  2,               0L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.OK},
                {11,                    10,                 2,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true}, //new IllegalArgumentException
                {5,                     6,                  7,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true},
                {7,                     6,                  7,               0L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.NotEnoughBookiesException},
                {1,                     2,                  2,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true}







        });
    }


    @Before
    public void set_up(){
        switch (this.clientConfType){
            case STD_CONF:
                break;
            case NO_STD_CONF:
                bkc.getConf().setOpportunisticStriping(true);
                bkc.getConf().setStoreSystemtimeAsLedgerCreationTime(true);
                break;
        }


    }

    @Test
    public void Test_InitiateAdv() {

        try {
            BookKeeper bookkeeper = this.bkc;
            Counter counter = new Counter();
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


    public static MyCallback createCallback() {

        return spy(new MyCallback() {

            @Override
            public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("Create complete: rc = " + rc + " counter value: " + counter.i);
            }

        });
    }

}