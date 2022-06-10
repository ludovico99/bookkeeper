package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BookKeeper;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetNumPendingRequestsTest extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;

    //Test: getNumPendingRequests(BookieId address, long ledgerId)
    private BookieClientImpl bookieClientImpl;
    private Object expectedNumPendingRequests;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private Boolean expectedNullPointerExNumPendingRequests = false;
    private Long numberPendingRequestToInsert = 1L;


    public BookieClientImplGetNumPendingRequestsTest(ParamType bookieId, ParamType ledgerId , Object expectedNumPendingRequests) {
        super(1);
        configureGetNumPendingRequests(bookieId, ledgerId, expectedNumPendingRequests);
    }

    private void configureGetNumPendingRequests(ParamType bookieId, ParamType ledgerId, Object expectedValue) {

        this.bookieIdParamType = bookieId;
        this.numberPendingRequestToInsert = 1L;

        try {

            ClientConfiguration confGetNumPendingRequests = TestBKConfiguration.newClientConfiguration();

            this.bookieClientImpl = new BookieClientImpl(confGetNumPendingRequests, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);


            switch (bookieId){
                case VALID_INSTANCE:
                    this.expectedNumPendingRequests = expectedValue;
                    this.numberPendingRequestToInsert = (Long) expectedValue;
                    break;

                case NULL_INSTANCE:
                    this.expectedNumPendingRequests = expectedValue;
                    this.bookieId = null;
                    this.expectedNullPointerExNumPendingRequests = true;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    this.expectedNumPendingRequests = expectedValue;
                    break;
            }

            switch (ledgerId) {
                case VALID_INSTANCE:
                    this.ledgerId = 0L;
                    break;

                case NULL_INSTANCE:
                    this.ledgerId = null;
                    this.expectedNullPointerExNumPendingRequests = true;
                    this.numberPendingRequestToInsert = 1L;
                    break;

                case INVALID_INSTANCE:
                    this.ledgerId = -1L;
                    break;

            }


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }
    }

    @Before
    public void set_up() throws Exception {

        BookieServer bookieServer = serverByIndex(0);

        if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = serverByIndex(0).getBookieId();

        bookieServer.getBookie().getLedgerStorage().setMasterKey(0,
                    "masterKey".getBytes(StandardCharsets.UTF_8));

        AsyncCallback.CreateCallback cb = mockCreateCallback();

        bkc.asyncCreateLedger(3,2, BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8)
        ,cb,new Object());

        DefaultPerChannelBookieClientPool pool = (DefaultPerChannelBookieClientPool) bookieClientImpl.
                lookupClient(bookieServer.getBookieId());

        BookkeeperInternalCallbacks.WriteCallback cb2 = mockWriteCallback();

        spyDefaultPerChannelBookieClientPool(pool.clients[0]);

        ByteBuf byteBuf = Unpooled.buffer("example".getBytes(StandardCharsets.UTF_8).length);
        ByteBufList byteBufList = ByteBufList.get(byteBuf);
        EnumSet<WriteFlag> writeFlags = EnumSet.allOf(WriteFlag.class);

        verify(cb).createComplete(isA(Integer.class), any(),
                isA(Object.class)); //Ho almeno un ledger

        //Per continuare dovrei assicurarmi di avere un ledger


        for (long i = 0; i< numberPendingRequestToInsert; i++) {
            pool.clients[0].addEntry(0L, bookieServer.getBookie().getLedgerStorage().readMasterKey(0L),
                    0, byteBufList, cb2, new Object(), 0, false, writeFlags);
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // Bookie Id,     ledger Id,   expectedNumPendingRequests
                {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE,          10L},
                {ParamType.INVALID_INSTANCE,   ParamType.INVALID_INSTANCE,       0L},
                {ParamType.NULL_INSTANCE,   ParamType.VALID_INSTANCE,        new NullPointerException()},
                {ParamType.VALID_INSTANCE,   ParamType.NULL_INSTANCE,        new NullPointerException()},
                {ParamType.NULL_INSTANCE,   ParamType.NULL_INSTANCE,         new NullPointerException()},
                {ParamType.VALID_INSTANCE,   ParamType.INVALID_INSTANCE,        0L},

        }) ;
    }

    @After
    public void tear_down() throws Exception {

        this.bookieClientImpl.close();
        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();
        }

    }

    @Test
    public void test_getNumPendingRequests() {

        if (this.exceptionInConfigPhase) Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            if (expectedNullPointerExNumPendingRequests) {
                try {
                    bookieClientImpl.getNumPendingRequests(this.bookieId, this.ledgerId);
                } catch (NullPointerException e) {
                    Assert.assertEquals("Exception that i expect is raised", this.expectedNumPendingRequests.getClass(), e.getClass());
                }
            } else Assert.assertEquals((long)this.expectedNumPendingRequests,
                     bookieClientImpl.getNumPendingRequests(this.bookieId, this.ledgerId));

        }
    }

    private void spyDefaultPerChannelBookieClientPool(PerChannelBookieClient client) {

        client = spy(client);

        doNothing().when(client).errorOut(any());
        doNothing().when(client).errorOut(any(),anyInt());
        doNothing().when(client).checkTimeoutOnPendingOperations();

    }

    public BookkeeperInternalCallbacks.WriteCallback mockWriteCallback(){
        BookkeeperInternalCallbacks.WriteCallback cb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(cb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(BookieId.class),
                isA(Object.class));

        return cb;
    }

    public AsyncCallback.CreateCallback mockCreateCallback(){
        AsyncCallback.CreateCallback cb = mock(AsyncCallback.CreateCallback.class);
        doNothing().when(cb).createComplete(isA(Integer.class), any(),
                isA(Object.class));

        return cb;
    }


}
