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
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public  class LedgerCreateOpInitiateTest extends BookKeeperClusterTestCase {


    private ClientConfType clientConfType;
    private Object expectedValue;
    private Integer ensembleSize;
    private Integer writeQuorumSize;
    private Integer ackQuorumSize;
    private AsyncCallback.CreateCallback cb;



        public LedgerCreateOpInitiateTest(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, ParamType cb, ClientConfType clientConfType, Object expectedValue) {
            super(3);
            configureInitiate(ensembleSize, writeQuorumSize, ackQuorumSize, cb, clientConfType, expectedValue);
        }

        private void configureInitiate(Integer ensembleSize, Integer writeQuorumSize, Integer ackQuorumSize, ParamType cb, ClientConfType clientConfType, Object expectedException) {

            this.expectedValue = expectedException;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.clientConfType = clientConfType;

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
                    //ensembleSize,writeQuorumSize,ackQuorumSize, CB, bk conf,      Exception
                    {null, 1, 1, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true},
                    {3, null, 1, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true},
                    {3, 1, null, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, true},

                    {1, 1, 1, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.OK},
                    {3, 2, 2, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF,BKException.Code.OK},

                    {1, 2, 2, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.ZKException},
                    {3, 4, 5, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.ZKException},
                    {3, 2, 3, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.ZKException},

                    {11, 10, 2, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},
                    {5, 6, 7, ParamType.VALID_INSTANCE, ClientConfType.STD_CONF, BKException.Code.NotEnoughBookiesException},

                    {3, 2, 2, ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF,BKException.Code.OK},
                    {11, 10, 2, ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, BKException.Code.NotEnoughBookiesException},
                    {5, 6, 7, ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true},
                    {1, 2, 2, ParamType.VALID_INSTANCE, ClientConfType.NO_STD_CONF, true} //???? ensemble è null????

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
        public void Test_Initiate() {

            try {
                BookKeeper bookkeeper = this.bkc;

                Counter counter = new Counter();
                LedgerCreateOp ledgerCreateOp = new LedgerCreateOp(bookkeeper, ensembleSize, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32,
                        "pwd".getBytes(StandardCharsets.UTF_8), this.cb, counter, null, EnumSet.allOf(WriteFlag.class), bookkeeper.getClientCtx().getClientStats());

                if ((int) this.expectedValue == BKException.Code.ZKException) {
                    // Non va i busy waiting poichè mi aspetto di non avere risposta !!!
                    ledgerCreateOp.initiate();
                    verifyNoInteractions(this.cb);
                } else {
                    counter.inc();

                    ledgerCreateOp.initiate();

                    counter.wait(0);

                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.cb).createComplete(argument.capture(), nullable(LedgerHandle.class), isA(Object.class));
                    Assert.assertEquals(this.expectedValue, argument.getValue());
                }


            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("Exception that i expect is raised", (boolean) this.expectedValue);
            }

        }


}


