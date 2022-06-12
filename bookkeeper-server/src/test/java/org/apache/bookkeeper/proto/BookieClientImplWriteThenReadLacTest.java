package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
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
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.awt.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplWriteThenReadLacTest extends BookKeeperClusterTestCase {

    private static Boolean exceptionInConfigPhase = false;
    private static ClientConfiguration clientConf;
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
                this.readLacCallback = mockReadLacCallback();
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
        clientConf = TestBKConfiguration.newClientConfiguration();
        bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
    }



    @Before
    public void set_up() {

        try {

            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));

            ByteBuf byteBuf= Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8));
            ByteBufList bufList = ByteBufList.get(byteBuf);

            bookieServer.getBookie().addEntry(byteBuf,true,
                    mockWriteCallback(),new Object(),"masterKey".getBytes(StandardCharsets.UTF_8));

            bookieClientImpl.addEntry(bookieId, handle.getLedgerMetadata().getLedgerId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                    0L,bufList , mockWriteCallback(), new Object(), BookieProtocol.ADDENTRY,false, WriteFlag.NONE);

            bookieClientImpl.writeLac(bookieId, handle.getLedgerMetadata().getLedgerId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                    handle.getLastAddConfirmed(), ByteBufList.get(Unpooled.buffer("This is the entry content".getBytes(StandardCharsets.UTF_8).length)), mockWriteLacCallback(), new Object());

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId =bookieId;
            if(ledgerIdParamType.equals(ParamType.VALID_INSTANCE)) this.ledgerId = handle.getLedgerMetadata().getLedgerId();

            if(this.bookieIdParamType.equals(ParamType.CLOSED_CONFIG)) this.bookieClientImpl.close();

        }catch (Exception e){
            exceptionInConfigPhase = true;
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
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Object(),       BKException.Code.OK},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.NULL_INSTANCE,  new Object(),        true},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.NULL_INSTANCE,  new Object(),        false},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.NULL_INSTANCE,  new Object(),        false},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE, new Object(),        false},
                { ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.CLOSED_CONFIG,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.CLOSED_CONFIG,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true}
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
    public void test_ReadLac() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {
                bookieClientImpl.readLac(this.bookieId, this.ledgerId, this.readLacCallback, this.ctx);

                while(bookieClientImpl.getNumPendingRequests(this.bookieId,this.ledgerId) != 0);

                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.readLacCallback).readLacComplete(argument.capture(), isA(Long.class), isA(ByteBuf.class), isA(ByteBuf.class), isA(Object.class));
                    Assert.assertEquals(this.expectedReadLac, argument.getValue());

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedReadLac);
            }

        }
    }


    public BookkeeperInternalCallbacks.WriteLacCallback mockWriteLacCallback(){
        BookkeeperInternalCallbacks.WriteLacCallback cb = mock(BookkeeperInternalCallbacks.WriteLacCallback.class);
        doNothing().when(cb).writeLacComplete(isA(Integer.class),isA(long.class),isA(BookieId.class),
                isA(Object.class));

        return cb;
    }

    public BookkeeperInternalCallbacks.ReadLacCallback mockReadLacCallback(){
        BookkeeperInternalCallbacks.ReadLacCallback cb = mock(BookkeeperInternalCallbacks.ReadLacCallback.class);
        doNothing().when(cb).readLacComplete(isA(Integer.class), isA(Long.class), isA(ByteBuf.class), isA(ByteBuf.class),
                isA(Object.class));

        return cb;
    }

    public BookkeeperInternalCallbacks.WriteCallback mockWriteCallback(){
        BookkeeperInternalCallbacks.WriteCallback cb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(cb).writeComplete(isA(Integer.class),isA(long.class),isA(long.class),isA(BookieId.class),
                isA(Object.class));

        return cb;
    }



}
