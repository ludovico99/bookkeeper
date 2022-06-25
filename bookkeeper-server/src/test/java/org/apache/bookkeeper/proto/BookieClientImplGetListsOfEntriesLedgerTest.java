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

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetListsOfEntriesLedgerTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;


    //Test:   CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieId address,
    //            long ledgerId)

    private Object expectedGetListsOfEntriesLedger;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private OrderedExecutor orderedExecutor;
    private ClientConfType clientConfType;
    private BookieId bookieId;
    private int lastRC = -1;




    public BookieClientImplGetListsOfEntriesLedgerTest(ParamType bookieId, long ledgerId, ClientConfType clientConfType, Object expected) {
        super(3);
        configureAddThenRead(bookieId, ledgerId, clientConfType, expected);

    }


    private void configureAddThenRead(ParamType bookieId, long ledgerId, ClientConfType clientConfType, Object expected) {

        this.bookieIdParamType = bookieId;
        this.ledgerId = ledgerId;
        this.clientConfType = clientConfType;
        this.expectedGetListsOfEntriesLedger = expected;

        try {

            this.orderedExecutor = OrderedExecutor.newBuilder().build();

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT,this.orderedExecutor, Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            switch (bookieId) {
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    break;

            }

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }


    }



    @Before
    public void set_Up() {

        try {

            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));
            //Sincrona
            long entryId = handle.addEntry("Adding Entry ".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));

            Counter counter = new Counter();

            while(this.lastRC != BKException.Code.OK) {
                counter.i = 1;

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);
            }

            if(this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = bookieId;

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
                            , bookieClientImpl, bookieId, 1);

                    pool2.clients[0].close();
                    this.bookieClientImpl.channels.put(bookieId, pool2);
                    this.orderedExecutor.shutdown();
                    break;
            }

            Utils.sleep(1000); //Inserisco una sleep nella speranza che la richieste nel frattempo sia processata

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //Bookie_ID                 Ledger_id     Client config
                {  ParamType.VALID_INSTANCE,     0L,      ClientConfType.STD_CONF, 1L},
                {  ParamType.VALID_INSTANCE,     -5L,     ClientConfType.STD_CONF, true},

                {  ParamType.INVALID_INSTANCE,   0L,      ClientConfType.STD_CONF, true},
                {  ParamType.INVALID_INSTANCE,   -5L,     ClientConfType.STD_CONF, true},

                {  ParamType.NULL_INSTANCE,      0L,      ClientConfType.STD_CONF,  true},
                {  ParamType.NULL_INSTANCE,      -5L,     ClientConfType.STD_CONF,  true},

                {  ParamType.VALID_INSTANCE,     0L,      ClientConfType.INVALID_CONFIG, true},

                {  ParamType.VALID_INSTANCE,     0L,      ClientConfType.REJECT_CONFIG,  true},

                {  ParamType.VALID_INSTANCE,     0L,      ClientConfType.CLOSED_CONFIG,  true},

        }) ;
    }



    @Test
    public void test_GetListsOfEntriesLedger() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                AvailabilityOfEntriesOfLedger entriesOfLedger = this.bookieClientImpl.getListOfEntriesOfLedger(this.bookieId, this.ledgerId).join();
                Assert.assertEquals(this.expectedGetListsOfEntriesLedger ,entriesOfLedger.getTotalNumOfAvailableEntries());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (boolean) this.expectedGetListsOfEntriesLedger);
            }

        }
    }


    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            Counter counter = (Counter) ctx1;
            counter.dec();
            this.lastRC = rc;
            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }


}
