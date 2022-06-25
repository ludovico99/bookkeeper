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
public class BookieClientImplWriteLacTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;

//    void writeLac(final BookieId addr, final long ledgerId, final byte[] masterKey,
//                  final long lac, final ByteBufList toSend, final BookkeeperInternalCallbacks.WriteLacCallback cb, final Object ctx)

    private BookkeeperInternalCallbacks.WriteLacCallback writeLacCallback;
    private Object ctx;
    private Object expectedWriteLac;
    private Long ledgerId;
    private ByteBufList toSend;
    private byte[] ms;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfType;
    private OrderedExecutor orderedExecutor;
    private BookieId bookieId;
    private long lac;
    private int lastRc = -1;


    public BookieClientImplWriteLacTest(ParamType bookieId, long ledgerId, byte[] ms, long lac, ByteBufList toSend, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedWriteLac) {
        super(3);
        configureAdd(bookieId, ledgerId, ms, lac, toSend, cb, ctx, clientConfType, expectedWriteLac);

    }


    private void configureAdd(ParamType bookieId, long ledgerId, byte[] ms, long lac, ByteBufList toSend, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedAdd) {

        this.bookieIdParamType = bookieId;
        this.clientConfType = clientConfType;
        this.ctx = ctx;
        this.expectedWriteLac = expectedAdd;
        this.ms = ms;
        this.toSend = toSend;
        this.lac = lac;
        this.ledgerId = ledgerId;

        try {

            this.orderedExecutor = OrderedExecutor.newBuilder().build();

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, orderedExecutor, Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            switch (bookieId) {
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
                    this.writeLacCallback = writeLacCallback();
                    break;

                case NULL_INSTANCE:
                    this.writeLacCallback = null;
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
                    setMasterKey(handle.getId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));

            Counter counter = new Counter();

            while(this.lastRc != 0) {
                counter.i = 1;

                System.out.println("Retry");

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);

                System.out.println("Add entry completed");
            }

            if(bookieIdParamType.equals(ParamType.VALID_INSTANCE))      this.bookieId = bookieId;


            switch (this.clientConfType){
                case STD_CONF:
                    break;
                case CLOSED_CONFIG:
                    this.bookieClientImpl.close();
                    break;
                case REJECT_CONFIG:
                    DefaultPerChannelBookieClientPool pool2 = new DefaultPerChannelBookieClientPool(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1)
                            , this.bookieClientImpl, bookieId, 1);

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


        byte[] validMasterKey = "masterKey".getBytes(StandardCharsets.UTF_8);
        byte[] notValidMasterKey = "noPwd".getBytes(StandardCharsets.UTF_8);

        ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
        ByteBufList byteBufList = ByteBufList.get(byteBuf);

        ByteBufList emptyByteBufList = ByteBufList.get(Unpooled.EMPTY_BUFFER);


        return Arrays.asList(new Object[][]{
                //Bookie_ID                   Ledger_id,   Master key   LAC    toSend,            WriteLacCallBack,         Object           ClientConf                        Raise exception
                {  ParamType.VALID_INSTANCE,     0L,   validMasterKey,   0L,   byteBufList,       ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.INVALID_INSTANCE,   0L,   validMasterKey,   0L,   byteBufList,       ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.BookieHandleNotAvailableException},
                {  ParamType.VALID_INSTANCE,     -5L,  validMasterKey,   0L,   byteBufList,       ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.VALID_INSTANCE,      0L,  validMasterKey,   -5L,  byteBufList,       ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.NULL_INSTANCE,       0L,  validMasterKey,   -5L,  byteBufList,       ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      0L,  validMasterKey,   0L,   null,              ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      0L,  validMasterKey,   0L,   emptyByteBufList,  ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          BKException.Code.WriteException},
                {  ParamType.VALID_INSTANCE,      0L,  notValidMasterKey,0L,   byteBufList,       ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          BKException.Code.UnauthorizedAccessException},
                {  ParamType.VALID_INSTANCE,      0L,   validMasterKey,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.REJECT_CONFIG,     BKException.Code.InterruptedException},
                {  ParamType.VALID_INSTANCE,      0L,   validMasterKey,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.CLOSED_CONFIG,     BKException.Code.ClientClosedException}


        }) ;
    }


    @Test
    public void test_WriteLac() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                ((Counter)this.ctx).inc();

                this.bookieClientImpl.writeLac(this.bookieId, this.ledgerId, this.ms,
                        this.lac, this.toSend, this.writeLacCallback, ctx);

                ((Counter)this.ctx).wait(0);

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                verify(this.writeLacCallback).writeLacComplete(argument.capture(),nullable(Long.class),
                        nullable(BookieId.class), isA(Object.class));
                Assert.assertEquals(this.expectedWriteLac, argument.getValue());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedWriteLac);
            }

        }
    }


    private BookkeeperInternalCallbacks.WriteLacCallback writeLacCallback(){

        return spy(new BookkeeperInternalCallbacks.WriteLacCallback() {

            @Override
            public void writeLacComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("WRITE LAC: rc = " + rc + " for ledger: " + ledgerId + " at bookie: " + addr);
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
