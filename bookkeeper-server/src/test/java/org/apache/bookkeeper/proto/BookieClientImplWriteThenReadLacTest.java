package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplWriteThenReadLacTest extends BookKeeperClusterTestCase {

    private static Boolean exceptionInConfigPhase = false;
    private static BookieClientImpl bookieClientImpl;


    //Test: readLac(final BookieId addr, final long ledgerId, final ReadLacCallback cb,
    //            final Object ctx)
    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback;
    private Object ctx;
    private Object expectedReadLac;
    private Long ledgerId;
    private ParamType ledgerIdParamType;
    private ParamType bookieIdParamType;
    private BookieId bookieId;



    public BookieClientImplWriteThenReadLacTest(ParamType bookieId, ParamType ledgerId , ParamType cb, Object ctx, Object expectedWriteLac) {
        super(3);
        configureReadLac(bookieId, ledgerId, cb, ctx, expectedWriteLac);
    }

    private void configureReadLac(ParamType bookieId, ParamType ledgerId, ParamType cb, Object ctx, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.ctx = ctx;
        this.expectedReadLac = expectedReadLac;
        this.ledgerIdParamType = ledgerId;

        switch (bookieId){
            case VALID_INSTANCE:
                break;

            case NULL_INSTANCE:
                this.bookieId = null;
                break;

            case INVALID_INSTANCE:
                this.bookieId = BookieId.parse("Bookie");
                break;

            case CLOSED_CONFIG:
                this.bookieId = BookieId.parse("Bookie");
                break;

        }

        switch (ledgerId) {
            case VALID_INSTANCE:
                break;

            case NULL_INSTANCE:
                this.ledgerId = null;
                break;

            case INVALID_INSTANCE:
                this.ledgerId = -1L;
                break;

        }

        switch (cb) {
            case VALID_INSTANCE:
                this.readLacCallback = readLacCallback();
                break;

            case NULL_INSTANCE:
                this.readLacCallback = null;
                break;

            case INVALID_INSTANCE:
               this.readLacCallback = (rc, ledgerId1, lac, buffer, ctx1) -> {
               };
                break;
        }

    }

    private static void setBookieClientImpl() throws IOException {
        bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
    }



    @Before
    public void set_up() {

        try {
            Counter counter = new Counter();

            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));

            counter.inc();
            handle.asyncAddEntry("Adding Entry ".getBytes(StandardCharsets.UTF_8),addCallback(),counter);

            counter.wait(0);

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));

            counter = new Counter();
            counter.inc();

//            bookieClientImpl.addEntry(bookieId, handle.getLedgerMetadata().getLedgerId(), "masterKey".getBytes(StandardCharsets.UTF_8),
//                    0L,byteBufList , mockWriteCallback(), new Object(), BookieProtocol.ADDENTRY,false, WriteFlag.NONE);

            ByteBuf byteBuf = Unpooled.wrappedBuffer("Last add confirmed write".getBytes(StandardCharsets.UTF_8));
            ByteBufList byteBufList = ByteBufList.get(byteBuf);

            bookieClientImpl.writeLac(bookieId, handle.getLedgerMetadata().getLedgerId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                    handle.getLastAddConfirmed(), byteBufList, writeLacCallback(), counter);

            counter.wait(0);

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId =bookieId;
            if(ledgerIdParamType.equals(ParamType.VALID_INSTANCE)) this.ledgerId = handle.getLedgerMetadata().getLedgerId();

            if(this.bookieIdParamType.equals(ParamType.CLOSED_CONFIG)) bookieClientImpl.close();

        }catch (Exception e){
            e.printStackTrace();
            //exceptionInConfigPhase = false;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        try {
            setBookieClientImpl();
        }catch (Exception e){
            e.printStackTrace();
            exceptionInConfigPhase = true;
        }

        return Arrays.asList(new Object[][]{
                //Bookie_ID                         Led_ID                          ReadLacCallback                   ctx        RaiseException
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Counter(),       BKException.Code.OK},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Counter(),        true},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.VALID_INSTANCE,  new Counter(),        true},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE, new Counter(),        BKException.Code.NoSuchEntryException},
                { ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Counter(),        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.CLOSED_CONFIG,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Counter(),        BKException.Code.ClientClosedException},
                { ParamType.CLOSED_CONFIG,       ParamType.INVALID_INSTANCE,           ParamType.VALID_INSTANCE, new Counter(),        BKException.Code.ClientClosedException}
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
    public void test_ReadLacAfterWrite() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {
                ((Counter)this.ctx).inc();

                bookieClientImpl.readLac(this.bookieId, this.ledgerId, this.readLacCallback, this.ctx);

                ((Counter)this.ctx).wait(0);

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                verify(this.readLacCallback).readLacComplete(argument.capture(), anyLong(), nullable(ByteBuf.class),
                        nullable(ByteBuf.class), isA(Object.class));
                Assert.assertEquals(this.expectedReadLac, argument.getValue());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedReadLac);
            }

        }
    }


    private BookkeeperInternalCallbacks.WriteLacCallback writeLacCallback(){

        return new BookkeeperInternalCallbacks.WriteLacCallback() {

            @Override
            public void writeLacComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
                BookieClientImplWriteThenReadLacTest.Counter counter = (BookieClientImplWriteThenReadLacTest.Counter) ctx;
                counter.dec();
                System.out.println("WRITE LAC: rc = " + rc + " for ledger: " + ledgerId + " at bookie: " + addr);
            }



        };

    }

    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback(){

        return spy(new BookkeeperInternalCallbacks.ReadLacCallback() {

            @Override
            public void readLacComplete(int rc, long ledgerId, ByteBuf lac, ByteBuf buffer, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("READ Lac: rc = " + rc  + "  ledger: " + ledgerId);
            }



        });
    }

    private AsyncCallback.AddCallback addCallback(){

        return new AsyncCallback.AddCallback() {

            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("ADD: rc = " + rc + " entry : " + entryId + " at ledger: " + lh.getId());
            }


        };
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
