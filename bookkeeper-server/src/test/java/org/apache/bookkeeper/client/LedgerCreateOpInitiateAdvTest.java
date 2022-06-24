package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Before;
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
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class LedgerCreateOpInitiateAdvTest extends BookKeeperClusterTestCase {

    private Object expectedValue;
    private Integer ensembleSize;
    private Integer writeQuorumSize;
    private Integer ackQuorumSize;
    private Long ledgerId;
    private AsyncCallback.CreateCallback cb;
    private ClientConfType clientConfType;
    private boolean exceptionInConfigPhase = false;

    public LedgerCreateOpInitiateAdvTest(int ensembleSize, int writeQuorumSize, int ackQuorumSize, long ledgerId, ParamType cb, ClientConfType clientConfType, Object expectedValue) {
        super(3);
        configureInitiateAdv(ensembleSize, writeQuorumSize, ackQuorumSize, ledgerId, cb,clientConfType, expectedValue);
    }


    private void configureInitiateAdv(int ensembleSize, int writeQuorumSize, int ackQuorumSize, long ledgerId, ParamType cb, ClientConfType clientConfType, Object expectedException) {
        this.clientConfType = clientConfType;
        this.expectedValue = expectedException;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.ledgerId = ledgerId;

        switch (cb) {
            case VALID_INSTANCE:
                this.cb = spy(MyCallback.class);
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
                //ensembleSize,writeQuorumSize,ackQuorumSize,ledgerId,CB,Client conf,Exception

                {-1, 0, 1,  0L, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF, new IllegalArgumentException()},
                {-1, 0, 0, -1L, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF, new IllegalArgumentException()},
                {-1, -2, -1,0L, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF, new IllegalArgumentException()},
                {-1, -2, -2,0L, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF, new IllegalArgumentException()},

                {1, 2, 3,  -1L,   ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,  BKException.Code.ZKException},
                {1, 2, 1,  0L,   ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,   new IllegalArgumentException()},
                {1, 0, -1, -1L, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,    BKException.Code.ZKException},
                {1, 0, 0,  0L,   ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,   BKException.Code.OK},

                {4, 5,  6,0L, ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {4, 5,  5,0L, ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {4, 3,  4,0L, ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {4, 3,  3,-1L,ParamType.VALID_INSTANCE,  ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},

                {4, 3, 3,0L, ParamType.VALID_INSTANCE,   ClientConfType.NO_STD_CONF, BKException.Code.OK},
                {4, 4, 3,0L, ParamType.VALID_INSTANCE,   ClientConfType.NO_STD_CONF, BKException.Code.NotEnoughBookiesException},
                {1, 0, 0,-1L,ParamType.VALID_INSTANCE,  ClientConfType.NO_STD_CONF, BKException.Code.OK},
                {4, 5, 6,0L, ParamType.VALID_INSTANCE,   ClientConfType.NO_STD_CONF, new NullPointerException()},
                {1, 2, 1,0L, ParamType.VALID_INSTANCE,   ClientConfType.NO_STD_CONF, new NullPointerException()},
                {-1, 0, 0,0L,ParamType.VALID_INSTANCE,   ClientConfType.NO_STD_CONF, new NullPointerException()}

        });
    }


    @Before
    public void set_up(){

        try {
            switch (this.clientConfType) {
                case STD_CONF:
                    break;
                case NO_STD_CONF:
                    bkc.getConf().setOpportunisticStriping(true);
                    bkc.getConf().setStoreSystemtimeAsLedgerCreationTime(true);
                    break;
            }

        }catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true; //Ci sono tanti errori nella connessione con Zookkeeper
        }


    }

    @Test
    public void Test_InitiateAdv() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {
                BookKeeper bookkeeper = this.bkc;
                Counter counter = new Counter();
                LedgerCreateOp ledgerCreateOp = new LedgerCreateOp(bookkeeper, ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                        "pwd".getBytes(StandardCharsets.UTF_8), this.cb, counter, null, EnumSet.allOf(WriteFlag.class), bookkeeper.getClientCtx().getClientStats());


                counter.inc();

                ledgerCreateOp.initiateAdv(this.ledgerId);

                if(this.expectedValue instanceof Integer) {
                    if(((int) this.expectedValue != BKException.Code.ZKException)) {
                        counter.wait(0);
                        ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                        verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class), isA(Object.class));
                        Assert.assertEquals(this.expectedValue, argument.getValue());
                    }
                    else verifyNoInteractions(this.cb);
                }
                else Assert.fail("Test case has failed"); Assert.fail("Test case has failed");
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.assertEquals("Exception that i expect is raised", this.expectedValue.getClass(), e.getClass());

            }

        }
    }


}