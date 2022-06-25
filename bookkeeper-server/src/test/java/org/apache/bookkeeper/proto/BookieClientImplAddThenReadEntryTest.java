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
public class BookieClientImplAddThenReadEntryTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;


    //Test:   readEntry(BookieId addr, long ledgerId, long entryId,
    //                          ReadEntryCallback cb, Object ctx, int flags)
    private BookkeeperInternalCallbacks.ReadEntryCallback readCallback;
    private int flags;
    private Object ctx;
    private Object expectedRead;
    private long ledgerId;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfTypeEnum;
    private BookieId bookieId;
    private long entryId;
    private int lastRC = -1;




    public BookieClientImplAddThenReadEntryTest(ParamType bookieId, long ledgerId , long entryId, ParamType cb, Object ctx, int flags, ClientConfType clientConfType, Object expectedReadLac) {
        super(3);
        configureAddThenRead(bookieId, ledgerId, entryId, cb, ctx, flags, clientConfType, expectedReadLac);

    }


    private void configureAddThenRead(ParamType bookieId, long ledgerId, long entryId, ParamType cb, Object ctx, int flags, ClientConfType clientConfType, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.clientConfTypeEnum = clientConfType;
        this.ctx = ctx;
        this.flags = flags;
        this.expectedRead = expectedReadLac;
        this.ledgerId = ledgerId;
        this.entryId = entryId;

        try {

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
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
                    this.readCallback = readCallback();
                    break;

                case NULL_INSTANCE:
                    this.readCallback = null;
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
            handle.addEntry("Adding Entry ".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));

            Counter counter = new Counter();

            while(this.lastRC != 0) {
                counter.i = 1;
                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        0L, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);
            }

            if(this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = bookieId;
            if(this.clientConfTypeEnum.equals(ClientConfType.CLOSED_CONFIG)) this.bookieClientImpl.close();

            Utils.sleep(2000); //Inserisco una sleep nella speranza che la richieste nel frattempo sia processata

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                    //Bookie_ID            Ledger_id   Entry_id   ReadEntryCallback           Object                 Flags           ClientConf                        Raise exception
                {  ParamType.VALID_INSTANCE,      0L,    0L,     ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ClientConfType.STD_CONF,          BKException.Code.OK},
                {  ParamType.INVALID_INSTANCE,    0L,    0L,     ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ClientConfType.STD_CONF,          BKException.Code.BookieHandleNotAvailableException},
                {  ParamType.VALID_INSTANCE,      -5L,   0L,     ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ClientConfType.STD_CONF,          BKException.Code.NoSuchLedgerExistsException},
                {  ParamType.VALID_INSTANCE,       0L,    -5L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ClientConfType.STD_CONF,          BKException.Code.TimeoutException},
                {  ParamType.NULL_INSTANCE,        0L,     5L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ClientConfType.STD_CONF,          true},
                {  ParamType.VALID_INSTANCE,      -5L,   -5L,    ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.READENTRY,  ClientConfType.STD_CONF,         BKException.Code.NoSuchLedgerExistsException},
                {  ParamType.VALID_INSTANCE,      0L,    0L,     ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.READENTRY,  ClientConfType.CLOSED_CONFIG,    BKException.Code.ClientClosedException}

        }) ;
    }


    @Test
    public void test_ReadAfterAdd() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                ((Counter)this.ctx).inc();

                this.bookieClientImpl.readEntry(this.bookieId, this.ledgerId, this.entryId, this.readCallback, this.ctx, this.flags,"masterKey".getBytes(StandardCharsets.UTF_8));

                ((Counter)this.ctx).wait(0);

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                verify(this.readCallback).readEntryComplete(argument.capture(), anyLong(), anyLong(),
                           nullable(ByteBuf.class), isA(Object.class));
                Assert.assertEquals(this.expectedRead, argument.getValue());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedRead);
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

    private BookkeeperInternalCallbacks.ReadEntryCallback readCallback(){

        return spy(new BookkeeperInternalCallbacks.ReadEntryCallback() {

            @Override
            public void readEntryComplete(int rc, long ledgerId1, long entryId1, ByteBuf buffer, Object ctx1) {
                Counter counter = (Counter) ctx1;
                counter.dec();
                System.out.println("READ: rc = " + rc + " for entry: " + entryId1 + " at ledger: " + ledgerId1);
            }
        });
    }

//    private AsyncCallback.AddCallback addCallback(){
//
//        return new AsyncCallback.AddCallback() {
//
//            @Override
//            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
//                Counter counter = (Counter) ctx;
//                counter.dec();
//                System.out.println("ADD: rc = " + rc + " entry : " + entryId + " at ledger: " + lh.getId());
//            }
//
//        };
//    }


}
