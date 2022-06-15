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
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ClientConfType;
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
public class BookieClientImplWriteLacTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;

//    void writeLac(final BookieId addr, final long ledgerId, final byte[] masterKey,
//                  final long lac, final ByteBufList toSend, final BookkeeperInternalCallbacks.WriteLacCallback cb, final Object ctx)

    private BookkeeperInternalCallbacks.WriteLacCallback writeLacCallback;;
    private Object ctx;
    private Object expectedWriteLac;
    private Long ledgerId;
    private ByteBufList toSend;
    private byte[] ms;
    private ParamType ledgerIdParamType;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfType;
    private OrderedExecutor orderedExecutor;
    private BookieId bookieId;
    private Long lac;
    private int lastRc = -1;


    public BookieClientImplWriteLacTest(ParamType bookieId, ParamType ledgerId, byte[] ms, ParamType lac, ByteBufList toSend, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedWriteLac) {
        super(3);
        configureAdd(bookieId, ledgerId, ms, lac, toSend, cb, ctx, clientConfType, expectedWriteLac);

    }


    private void configureAdd(ParamType bookieId, ParamType ledgerId, byte[] ms, ParamType lac, ByteBufList toSend, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedAdd) {

        this.bookieIdParamType = bookieId;
        this.ledgerIdParamType = ledgerId;
        this.clientConfType = clientConfType;
        this.ctx = ctx;
        this.expectedWriteLac = expectedAdd;
        this.ms = ms;
        this.toSend = toSend;

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

            switch (ledgerId) {
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.ledgerId = null;
                    break;

                case INVALID_INSTANCE:
                    this.ledgerId = -5L;
                    break;

            }

            switch (lac) {
                case VALID_INSTANCE:
                    this.lac = 0L;
                    break;

                case NULL_INSTANCE:
                    this.lac = null;
                    break;

                case INVALID_INSTANCE:
                    this.lac = -5L;
                    break;

            }

            switch (cb) {
                case VALID_INSTANCE:
                    this.writeLacCallback = writeLacCallback();
                    break;

                case NULL_INSTANCE:
                    this.writeLacCallback = null;
                    break;

                case INVALID_INSTANCE:
                    //to be determined
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
            counter.inc();

            while(this.lastRc != 0) {

                System.out.println("Retry");

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);

                System.out.println("Add entry completed");
            }

            if(bookieIdParamType.equals(ParamType.VALID_INSTANCE))      this.bookieId = bookieId;
            if(ledgerIdParamType.equals(ParamType.VALID_INSTANCE))      this.ledgerId = handle.getId();

            switch (clientConfType){
                case STD_CONF:
                    break;
                case CLOSED_CONFIG:
                    this.bookieClientImpl.close();
                    break;
                case INVALID_CONFIG:
                    DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1)
                            , bookieClientImpl, bookieId, 1);

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
                //Bookie_ID                       Ledger_id                    Master key        LAC                        toSend,                      ReadEntryCallback,         Object           ClientConf                        Raise exception
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.BookieHandleNotAvailableException},
                {  ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE, validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.INVALID_INSTANCE, byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.INVALID_INSTANCE, byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      ParamType.NULL_INSTANCE,    validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter() ,  ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.NULL_INSTANCE,    byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.CLOSED_CONFIG,     true},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.CLOSED_CONFIG,     BKException.Code.ClientClosedException},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   null,                        ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   emptyByteBufList,            ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          BKException.Code.WriteException},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   notValidMasterKey,ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          BKException.Code.UnauthorizedAccessException},
//                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   null,             ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.STD_CONF,          true},

                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.REJECT_CONFIG,           BKException.Code.InterruptedException},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,   validMasterKey,   ParamType.VALID_INSTANCE,   byteBufList,                 ParamType.VALID_INSTANCE,  new Counter(),   ClientConfType.INVALID_CONFIG,          BKException.Code.ClientClosedException}


        }) ;
    }


    @Test
    public void test_WriteLac() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {

                ((Counter)this.ctx).inc();

                bookieClientImpl.writeLac(this.bookieId, this.ledgerId, this.ms,
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
