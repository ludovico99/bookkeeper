package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.WriteFlag;
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

@Ignore
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
                //ensembleSize,       writeQuorumSize,     ackQuorumSize,    ledgerId,             CB,                         Client conf,      Exception

                {1,                     2,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {1,                     2,                  3,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {3,                     2,                  3,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException
                {3,                     2,                  1,             -2L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true}, //new IllegalArgumentException

                {3,                     2,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.OK},
                {3,                     2,                  2,             -1L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF,BKException.Code.OK},

                {10,                    5,                  2,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {5,                     6,                  7,              0L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                {5,                     6,                  7,             -2L,                 ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},

                {3,                     2,                  2,               0L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.OK},
                {11,                    10,                 2,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.NotEnoughBookiesException},
                {5,                     6,                  7,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true},
                {7,                     6,                  7,               0L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.NotEnoughBookiesException},
                {1,                     2,                  2,              -2L,                ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true}

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

                if (((int) this.expectedValue != BKException.Code.ZKException)) {
                    counter.wait(0);
                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class), isA(Object.class));
                    Assert.assertEquals(this.expectedValue, argument.getValue());
                } else verifyNoInteractions(this.cb);
            }
            catch (ClassCastException castException){
                Assert.fail("Cast exception raised means that the expected value is wrong");
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("Exception that i expect is raised", (boolean) this.expectedValue);

            }

        }
    }


}