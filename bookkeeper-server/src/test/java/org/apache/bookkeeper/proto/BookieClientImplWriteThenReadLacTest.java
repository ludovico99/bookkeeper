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
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
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

    private Boolean exceptionInConfigPhase = false;
    private BookieClientImpl bookieClientImpl;


    //Test: readLac(final BookieId addr, final long ledgerId, final ReadLacCallback cb,
    //            final Object ctx)
    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback;
    private Object ctx;
    private Object expectedReadLac;
    private Long ledgerId;
    private ParamType ledgerIdParamType;
    private ParamType bookieIdParamType;
    private  ClientConfType clientConfType;
    private BookieId bookieId;
    private Long LacEntryId;



    public BookieClientImplWriteThenReadLacTest(ParamType bookieId, ParamType ledgerId , ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedWriteLac) {
        super(3);
        configureReadLac(bookieId, ledgerId, cb, ctx,clientConfType, expectedWriteLac);
    }

    private void configureReadLac(ParamType bookieId, ParamType ledgerId, ParamType cb, Object ctx, ClientConfType clientConfType, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.ctx = ctx;
        this.expectedReadLac = expectedReadLac;
        this.ledgerIdParamType = ledgerId;
        this.clientConfType = clientConfType;

        try {


        this.bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
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
            counter.inc();

            while(this.LacEntryId == null) {

                System.out.println("Retry");

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);

                this.bookieClientImpl.addEntry(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        entryId, byteBufList, writeCallback(), counter, BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

                counter.wait(0);

                System.out.println("Add entry completed");
            }

            counter = new Counter();
            counter.inc();

            ByteBuf byteBuf2 = Unpooled.wrappedBuffer("Last add confirmed".getBytes(StandardCharsets.UTF_8));
            ByteBufList byteBufList2 = ByteBufList.get(byteBuf2);

            this.bookieClientImpl.writeLac(bookieId, handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                    this.LacEntryId, byteBufList2, writeLacCallback(), counter);

            counter.wait(0);

            if (this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId =bookieId;
            if(this.ledgerIdParamType.equals(ParamType.VALID_INSTANCE)) this.ledgerId = handle.getLedgerMetadata().getLedgerId();
            if(this.clientConfType.equals(ClientConfType.CLOSED_CONFIG)) this.bookieClientImpl.close();


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //Bookie_ID                         Led_ID                          ReadLacCallback                   ctx          client conf                     RaiseException
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        BKException.Code.OK},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        true},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.STD_CONF,        true},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.NoSuchEntryException},
                { ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Counter(),  ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE,  new Counter(), ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException}
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
                Counter counter = (Counter) ctx;
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

    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            Counter counter = (Counter) ctx1;
            counter.dec();

            if(rc == BKException.Code.OK) this.LacEntryId = entry;

            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }


}
