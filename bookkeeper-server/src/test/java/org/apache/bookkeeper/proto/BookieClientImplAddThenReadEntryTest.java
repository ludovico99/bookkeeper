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
    private ParamType msParamType;
    private byte[] ms;




    public BookieClientImplAddThenReadEntryTest(ParamType bookieId, long ledgerId , long entryId, ParamType cb, Object ctx, int flags, ParamType ms, ClientConfType clientConfType, Object expectedReadLac) {
        super(3);
        configureAddThenRead(bookieId, ledgerId, entryId, cb, ctx, flags,ms, clientConfType, expectedReadLac);

    }


    private void configureAddThenRead(ParamType bookieId, long ledgerId, long entryId, ParamType cb, Object ctx, int flags, ParamType ms, ClientConfType clientConfType, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.clientConfTypeEnum = clientConfType;
        this.ctx = ctx;
        this.flags = flags;
        this.expectedRead = expectedReadLac;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.msParamType = ms;

        try {

            this.setBaseClientConf(TestBKConfiguration.newClientConfiguration());

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

            switch (ms){
                case VALID_INSTANCE:
                    break;
                case INVALID_INSTANCE:
                    this.ms = "anotherMasterKey".getBytes(StandardCharsets.UTF_8);
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

            this.bookieClientImpl =  (BookieClientImpl) bkc.getBookieClient();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));

            Counter counter = new Counter();

            while(this.lastRC != 0) {
                counter.i = 1;
                ByteBuf toSend = Unpooled.buffer(1024);
                toSend.resetReaderIndex();
                toSend.resetWriterIndex();
                toSend.writeLong(handle.getId());
                toSend.writeLong(0L);
                toSend.writeBytes("Entry content".getBytes(StandardCharsets.UTF_8));
                toSend.writerIndex(toSend.capacity());

                bookieClientImpl.addEntry(bookieId, handle.getId(), bookieServer.getBookie().getLedgerStorage().readMasterKey(handle.getId()),
                        0L, ByteBufList.get(toSend), writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);
            }

            if(this.bookieIdParamType == ParamType.VALID_INSTANCE) this.bookieId = bookieId;
            if(this.clientConfTypeEnum == ClientConfType.CLOSED_CONFIG) this.bookieClientImpl.close();
            if(this.msParamType == ParamType.VALID_INSTANCE) this.ms = bookieServer.getBookie().getLedgerStorage().readMasterKey(handle.getId());

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                    //Bookie_ID            Ledger_id   Entry_id   ReadEntryCallback           Object                 Flags          masterKey,          ClientConf           Raise exception
                {  ParamType.VALID_INSTANCE,      0L,    0L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     BKException.Code.OK},
                {  ParamType.VALID_INSTANCE,      0L,    0L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.INVALID_INSTANCE,  ClientConfType.STD_CONF,     BKException.Code.OK},
                {  ParamType.INVALID_INSTANCE,    0L,    0L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     BKException.Code.BookieHandleNotAvailableException},
                {  ParamType.VALID_INSTANCE,     -5L,   0L,    ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     BKException.Code.NoSuchLedgerExistsException},
                {  ParamType.VALID_INSTANCE,      0L,   -5L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     BKException.Code.TimeoutException},
                {  ParamType.NULL_INSTANCE,       0L,    5L,   ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     true},
                {  ParamType.VALID_INSTANCE,     -5L,   -5L,   ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.STD_CONF,     BKException.Code.NoSuchLedgerExistsException},
                {  ParamType.VALID_INSTANCE,      0L,    0L,   ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.READENTRY, ParamType.VALID_INSTANCE,    ClientConfType.CLOSED_CONFIG,BKException.Code.ClientClosedException}

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

                this.bookieClientImpl.readEntry(this.bookieId, this.ledgerId, this.entryId, this.readCallback, this.ctx, this.flags, this.ms);

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



}
