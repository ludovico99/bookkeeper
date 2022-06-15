package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
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
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InjectMocks;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetNumPendingRequestsTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;
    private ClientConfiguration clientConf;

    //Test: getNumPendingRequests(BookieId address, long ledgerId)
    private long expectedNumPendingRequests;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfType;
    private BookieId bookieId;
    private Boolean expectedNullPointerExNumPendingRequests = false;
    private Long numberPendingRequestToInsert;


    public BookieClientImplGetNumPendingRequestsTest(ParamType bookieId, ParamType ledgerId, ClientConfType clientConfType) {
        super(3);
        configureGetNumPendingRequests(bookieId, ledgerId,clientConfType);
    }



    private void configureGetNumPendingRequests(ParamType bookieId, ParamType ledgerId,ClientConfType clientConfType) {

        this.bookieIdParamType = bookieId;
        this.numberPendingRequestToInsert = 20L;
        this.clientConfType = clientConfType;

        try {

            this.clientConf = TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1);

            this.bookieClientImpl = new BookieClientImpl(this.clientConf, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            switch (bookieId) {
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedNullPointerExNumPendingRequests = true;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    this.expectedNumPendingRequests = 0L;
                    break;
            }

            switch (ledgerId) {
                case VALID_INSTANCE:
                    this.ledgerId = 0L;
                    break;

                case NULL_INSTANCE:
                    this.ledgerId = null;
                    this.expectedNullPointerExNumPendingRequests = true;
                    break;

                case INVALID_INSTANCE:
                    this.ledgerId = -5L;
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

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pippo".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));


            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(this.clientConf, bookieClientImpl,
                    bookieId, 1);

            bookieClientImpl.channels.put(bookieId, pool);

            PerChannelBookieClient spyInstance = spy(bookieClientImpl.create(bookieId, pool,
                    SecurityProviderFactoryFactory.getSecurityProviderFactory(this.clientConf.getTLSProviderFactoryClass()), false));

            doNothing().when(spyInstance).errorOut(isA(PerChannelBookieClient.CompletionKey.class));
            doNothing().when(spyInstance).errorOut(isA(PerChannelBookieClient.CompletionKey.class), isA(int.class));
            doNothing().when(spyInstance).checkTimeoutOnPendingOperations();
            doNothing().when(spyInstance).channelRead(isA(ChannelHandlerContext.class),isA(Object.class));
            //In questo modo mi assicuro che le richieste non vengano eseguite

            Arrays.fill(pool.clients,spyInstance);

            Counter counter = new Counter();

            for (long i = 0; i < numberPendingRequestToInsert; i++) {
                counter.inc();

                handle.addEntry("hello".getBytes(StandardCharsets.UTF_8));

                ByteBuf byteBuf = Unpooled.wrappedBuffer("This is the entry content".getBytes(StandardCharsets.UTF_8));
                ByteBufList byteBufList = ByteBufList.get(byteBuf);


                bookieClientImpl.addEntry(bookieId,handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                        i, byteBufList, writeCallback(), counter , BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

            }

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)){
                this.bookieId = bookieId;
                this.expectedNumPendingRequests = numberPendingRequestToInsert;
            }

            if(clientConfType.equals(ClientConfType.CLOSED_CONFIG)){
                bookieClientImpl.close();
                this.expectedNumPendingRequests = 0L;
            }

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                // Bookie Id,                  ledger Id,                     Client conf type
                {ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,      ClientConfType.STD_CONF},
                {ParamType.INVALID_INSTANCE,   ParamType.INVALID_INSTANCE,    ClientConfType.STD_CONF},
                {ParamType.NULL_INSTANCE,      ParamType.VALID_INSTANCE,      ClientConfType.STD_CONF},
                {ParamType.VALID_INSTANCE,     ParamType.NULL_INSTANCE,       ClientConfType.STD_CONF},
                {ParamType.NULL_INSTANCE,      ParamType.NULL_INSTANCE,       ClientConfType.STD_CONF},
                {ParamType.VALID_INSTANCE,     ParamType.INVALID_INSTANCE,    ClientConfType.CLOSED_CONFIG},
                {ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,      ClientConfType.CLOSED_CONFIG}

        }) ;
    }


    @Test
    public void test_getNumPendingRequests() {

        if (exceptionInConfigPhase) Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            if (expectedNullPointerExNumPendingRequests) {
                try {
                    bookieClientImpl.getNumPendingRequests(this.bookieId, this.ledgerId);
                    Assert.fail("An exception was expected but hasn't been thrown");
                } catch (Exception e) {
                    Assert.assertTrue("Exception that i expect is raised", true);
                }
            } else {
                System.out.printf("PENDING REQUESTS EXPECTED: %d ", this.expectedNumPendingRequests);
                Assert.assertEquals(this.expectedNumPendingRequests,
                        bookieClientImpl.getNumPendingRequests(this.bookieId, this.ledgerId));
            }

        }
    }


    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            Counter counter = (Counter) ctx1;
            counter.dec();

            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }

}
