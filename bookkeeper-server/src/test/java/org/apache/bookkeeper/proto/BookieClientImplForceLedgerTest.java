package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplForceLedgerTest extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;
    private BookieClientImpl bookieClientImpl;


    //Test: forceLedger(final BookieId addr, final long ledgerId,
    //            final ForceLedgerCallback cb, final Object ctx)
    private BookkeeperInternalCallbacks.ForceLedgerCallback forceLedgerCallback;
    private Object ctx;
    private Object expectedForceLedger;
    private OrderedExecutor orderedExecutor;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private  ClientConfType clientConfType;
    private BookieId bookieId;
    private int lastRc;


    public BookieClientImplForceLedgerTest(ParamType bookieId, long ledgerId , ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedForceLedger) {
        super(3);
        configureForceLedger(bookieId, ledgerId, cb, ctx,clientConfType, expectedForceLedger);

    }

    private void configureForceLedger(ParamType bookieId, long ledgerId, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedForceLedger) {

        this.bookieIdParamType = bookieId;
        this.ctx = ctx;
        this.expectedForceLedger = expectedForceLedger;
        this.clientConfType = clientConfType;
        this.ledgerId = ledgerId;

        try {

            this.orderedExecutor = OrderedExecutor.newBuilder().build();

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT,this.orderedExecutor , Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);


            switch (bookieId){
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie");
                    break;

            }


            switch (cb) {
                case VALID_INSTANCE:
                    this.forceLedgerCallback = forceLedgerCallback();
                    break;

                case NULL_INSTANCE:
                    this.forceLedgerCallback = null;
                    break;

                case INVALID_INSTANCE:
                    //To be determined
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }




    @Before
    public void set_up() {

        try {

            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));
            //Sincrona
           handle.addEntry("Adding Entry ".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));




            if (this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId =bookieId;

            switch (clientConfType){
                case STD_CONF:
                    break;
                case CLOSED_CONFIG:
                    this.bookieClientImpl.close();
                    break;
                case INVALID_CONFIG:
                    DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1)
                            , this.bookieClientImpl, bookieId, 1);

                    pool.clients[0].close();
                    this.bookieClientImpl.channels.put(bookieId, pool);
                    break;
                case REJECT_CONFIG:
                    DefaultPerChannelBookieClientPool pool2 = new DefaultPerChannelBookieClientPool(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1)
                            , this.bookieClientImpl, bookieId, 1);

                    pool2.clients[0].close();
                    this.bookieClientImpl.channels.put(bookieId, pool2);
                    this.orderedExecutor.shutdown();
                    break;
            }

            Utils.sleep(5000); //Inserisco una sleep nella speranza che la richieste nel frattempo sia processata

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //Bookie_ID                    Led_ID          ForceLedgerCallback        ctx            client conf                     RaiseException
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        BKException.Code.OK},
                { ParamType.NULL_INSTANCE,       0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        true},
                { ParamType.VALID_INSTANCE,     -5L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        BKException.Code.OK},
                { ParamType.INVALID_INSTANCE,    0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.INVALID_INSTANCE,   -5L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      -5L,          ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.INVALID_CONFIG,  BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.REJECT_CONFIG,   BKException.Code.InterruptedException},
        }) ;
    }


    @Test
    public void test_forceLedger() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                ((Counter)this.ctx).inc();

                this.bookieClientImpl.forceLedger(this.bookieId, this.ledgerId, this.forceLedgerCallback, this.ctx);

                ((Counter)this.ctx).wait(0);

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                verify(this.forceLedgerCallback).forceLedgerComplete(argument.capture(), nullable(Long.class),
                        nullable(BookieId.class), isA(Object.class));
                Assert.assertEquals(this.expectedForceLedger, argument.getValue());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedForceLedger);
            }

        }
    }



    private BookkeeperInternalCallbacks.ForceLedgerCallback forceLedgerCallback(){

        return spy(new BookkeeperInternalCallbacks.ForceLedgerCallback() {

            @Override
            public void forceLedgerComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("Force ledger callback: rc = " + rc  + "  ledger: " + ledgerId);
            }

        });
    }

    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            Counter counter = (Counter) ctx1;
            counter.dec();
            this.lastRc = rc;
            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }


}
