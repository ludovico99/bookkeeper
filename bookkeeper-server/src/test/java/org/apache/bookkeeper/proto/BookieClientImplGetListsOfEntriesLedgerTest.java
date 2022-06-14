package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
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
public class BookieClientImplGetListsOfEntriesLedgerTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;


    //Test:   CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieId address,
    //            long ledgerId)

    private Object expectedGetListsOfEntriesLedger;
    private Long ledgerId;
    private ParamType ledgerIdParamType;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private int lastRC = -1;




    public BookieClientImplGetListsOfEntriesLedgerTest(ParamType bookieId, ParamType ledgerId) {
        super(3);
        configureAddThenRead(bookieId, ledgerId);

    }


    private void configureAddThenRead(ParamType bookieId, ParamType ledgerId) {

        this.bookieIdParamType = bookieId;
        this.ledgerIdParamType = ledgerId;

        try {

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            switch (bookieId) {
                case VALID_INSTANCE:
                    if (ledgerIdParamType.equals(ParamType.VALID_INSTANCE)) this.expectedGetListsOfEntriesLedger = 1L;
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedGetListsOfEntriesLedger = true;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    this.expectedGetListsOfEntriesLedger = true;
                    break;

            }

            switch (ledgerId) {
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.ledgerId = null;
                    this.expectedGetListsOfEntriesLedger = true;
                    break;

                case INVALID_INSTANCE:
                    this.ledgerId = -5L;
                    this.expectedGetListsOfEntriesLedger = true;
                    break;

            }

        }catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
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

            Counter counter = new Counter();;
            counter.inc();

            while(this.lastRC != 0) {

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);
            }

            if(bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = bookieId;
            if(ledgerIdParamType.equals(ParamType.VALID_INSTANCE)) this.ledgerId = handle.getId();


        }catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //Bookie_ID                       Ledger_id
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE},
                {  ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE},
                {  ParamType.VALID_INSTANCE,      ParamType.NULL_INSTANCE},

                {  ParamType.INVALID_INSTANCE,      ParamType.VALID_INSTANCE},
                {  ParamType.INVALID_INSTANCE,      ParamType.INVALID_INSTANCE},
                {  ParamType.INVALID_INSTANCE,      ParamType.NULL_INSTANCE},

                {  ParamType.NULL_INSTANCE,      ParamType.VALID_INSTANCE},
                {  ParamType.NULL_INSTANCE,      ParamType.INVALID_INSTANCE},
                {  ParamType.NULL_INSTANCE,      ParamType.NULL_INSTANCE}

        }) ;
    }

    @After
    public void tear_down() throws Exception {

        for (int i=0; i<numBookies; i++){
            serverByIndex(i).shutdown();
            serverByIndex(i).getBookie().shutdown();
        }

    }



    @Test
    public void test_ReadAfterAdd() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {

                AvailabilityOfEntriesOfLedger entriesOfLedger = bookieClientImpl.getListOfEntriesOfLedger(this.bookieId, this.ledgerId).join();

                Assert.assertEquals(this.expectedGetListsOfEntriesLedger,entriesOfLedger.getTotalNumOfAvailableEntries());


            } catch (Exception e){
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedGetListsOfEntriesLedger);
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
