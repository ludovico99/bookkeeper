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
public class BookieClientImplWriteThenReadLacTest extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;
    private BookieClientImpl bookieClientImpl;


    //Test: readLac(final BookieId addr, final long ledgerId, final ReadLacCallback cb,
    //            final Object ctx)
    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback;
    private Object ctx;
    private Object expectedReadLac;
    private long ledgerId;
    private ParamType bookieIdParamType;
    private int writeLacRC = -1;
    private ClientConfType clientConfType;
    private BookieId bookieId;
    private int writeRC = -1;
    private OrderedExecutor orderedExecutor;


    public BookieClientImplWriteThenReadLacTest(ParamType bookieId, long ledgerId , ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedWriteLac) {
        super(3);
        configureReadLac(bookieId, ledgerId, cb, ctx,clientConfType, expectedWriteLac);
    }

    private void configureReadLac(ParamType bookieId, long ledgerId, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.ctx = ctx;
        this.expectedReadLac = expectedReadLac;
        this.clientConfType = clientConfType;
        this.ledgerId = ledgerId;

        try {

            this.orderedExecutor = OrderedExecutor.newBuilder().build();

            this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, this.orderedExecutor, Executors.newSingleThreadScheduledExecutor(
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
                    this.readLacCallback = readLacCallback();
                    break;

                case NULL_INSTANCE:
                    this.readLacCallback = null;
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
            long entryId = handle.addEntry("Adding Entry ".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));

            Counter counter = new Counter();

            while(this.writeRC != BKException.Code.OK) {
                counter.i = 1;

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);
            }

            counter = new Counter();

            while(this.writeLacRC != BKException.Code.OK) {
                counter.i = 1;
                ByteBuf byteBuf2 = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList2 = ByteBufList.get(byteBuf2);

                this.bookieClientImpl.writeLac(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList2, writeLacCallback(), counter);

                counter.wait(0);
            }

            if (this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = bookieId;
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


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //Bookie_ID                     Led_ID         ReadLacCallback            ctx             client conf                     RaiseException
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.OK},
                { ParamType.NULL_INSTANCE,       0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        true},
                { ParamType.VALID_INSTANCE,     -5L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.NoSuchEntryException},
                { ParamType.INVALID_INSTANCE,    0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.INVALID_INSTANCE,   -5L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.INVALID_CONFIG,  BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      0L,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.REJECT_CONFIG,   BKException.Code.InterruptedException},
        }) ;
    }


    @Test
    public void test_ReadLacAfterWrite() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                ((Counter)this.ctx).inc();

                this.bookieClientImpl.readLac(this.bookieId, this.ledgerId, this.readLacCallback, this.ctx);

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

        return (rc, ledgerId, addr, ctx) -> {
            Counter counter = (Counter) ctx;
            counter.dec();
            this.writeLacRC = rc;
            System.out.println("\nWRITE LAC: rc = " + rc + " for ledger: " + ledgerId + " at bookie: " + addr);
        };

    }

    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback(){

        return spy(new BookkeeperInternalCallbacks.ReadLacCallback() {

            @Override
            public void readLacComplete(int rc, long ledgerId, ByteBuf lac, ByteBuf buffer, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                System.out.println("\nREAD Lac: rc = " + rc  + "  ledger: " + ledgerId);
            }
        });
    }

    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            Counter counter = (Counter) ctx1;
            counter.dec();
            this.writeRC = rc;
            System.out.println("\nWRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }


}
