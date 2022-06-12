package org.apache.bookkeeper.proto;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
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
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetNumPendingRequestsTest extends BookKeeperClusterTestCase {

    private static Boolean exceptionInConfigPhase = false;
    private static BookieClientImpl bookieClientImpl;
    private static ClientConfiguration clientConf;

    //Test: getNumPendingRequests(BookieId address, long ledgerId)
    private Object expectedNumPendingRequests;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private Boolean expectedNullPointerExNumPendingRequests = false;
    private Long numberPendingRequestToInsert = 1L;


    public BookieClientImplGetNumPendingRequestsTest(ParamType bookieId, ParamType ledgerId , Object expectedNumPendingRequests) {
        super(3);
        configureGetNumPendingRequests(bookieId, ledgerId, expectedNumPendingRequests);
    }

    private static void setBookieClientImpl() throws IOException {
        clientConf = TestBKConfiguration.newClientConfiguration();
        bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
    }

    private void configureGetNumPendingRequests(ParamType bookieId, ParamType ledgerId, Object expectedValue) {

        this.bookieIdParamType = bookieId;
        this.numberPendingRequestToInsert = 1L;

        switch (bookieId){
                case VALID_INSTANCE:
                    this.expectedNumPendingRequests = expectedValue;
                    if (!ledgerId.equals(ParamType.NULL_INSTANCE)) this.numberPendingRequestToInsert = (Long) expectedValue;
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

    }

    @Before
    public void set_up() throws Exception {
        try {
            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)) this.bookieId = bookieId;

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pippo".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));

            DefaultPerChannelBookieClientPool pool = (DefaultPerChannelBookieClientPool) bookieClientImpl.
                    lookupClient(bookieId);

            pool.clients[0] = spy(bookieClientImpl.create(bookieId, pool,
                    SecurityProviderFactoryFactory.getSecurityProviderFactory(clientConf.getTLSProviderFactoryClass()), false));

            doNothing().when(pool.clients[0]).errorOut(isA(PerChannelBookieClient.CompletionKey.class));
            doNothing().when(pool.clients[0]).errorOut(isA(PerChannelBookieClient.CompletionKey.class), isA(int.class));
            doNothing().when(pool.clients[0]).checkTimeoutOnPendingOperations();
            //Mi assicuro che nella concorrenza non siano rimossi

            EnumSet<WriteFlag> writeFlags = EnumSet.allOf(WriteFlag.class);

            for (long i = 0; i < numberPendingRequestToInsert; i++) {

                ByteBufList bufList = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8)));

                pool.clients[0].addEntry(handle.getLedgerMetadata().getLedgerId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        i, bufList, mockWriteCallback(), new Object(), 0, false, writeFlags);
            }
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

        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();

        }

    }


    @Test
    public void test_getNumPendingRequests() {

        if (exceptionInConfigPhase) Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
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


    public BookkeeperInternalCallbacks.WriteCallback mockWriteCallback(){
        BookkeeperInternalCallbacks.WriteCallback cb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(cb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(BookieId.class),
                isA(Object.class));

        return cb;
    }



}
