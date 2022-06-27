package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
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
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import static org.apache.bookkeeper.proto.BookieClient.PENDINGREQ_NOTWRITABLE_MASK;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetNumPendingRequestsTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;
    private ClientConfiguration clientConf;

    //Test: getNumPendingRequests(BookieId address, long ledgerId)
    private Object expectedNumPendingRequests;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfType;
    private BookieId bookieId;
    private Long numberPendingRequestToInsert;


    public BookieClientImplGetNumPendingRequestsTest(ParamType bookieId, long ledgerId, ClientConfType clientConfType,Object expected ) {
        super(3);
        configureGetNumPendingRequests(bookieId, ledgerId,clientConfType, expected);
    }



    private void configureGetNumPendingRequests(ParamType bookieId, long ledgerId,ClientConfType clientConfType, Object expected) {

        this.bookieIdParamType = bookieId;
        this.clientConfType = clientConfType;
        this.expectedNumPendingRequests = expected;
        this.ledgerId = ledgerId;
        this.numberPendingRequestToInsert = 10L;

        try {

            this.clientConf = TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1);
            this.setBaseClientConf(this.clientConf);

            switch (bookieId) {
                case VALID_INSTANCE:
                    if (clientConfType == ClientConfType.STD_CONF) {
                        this.numberPendingRequestToInsert = (Long) expected;
                    }

                case NULL_INSTANCE:
                    this.bookieId = null;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("");
                    break;
            }

        }catch (IllegalArgumentException ie){
            ie.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }

    @Before
    public void set_up() {
        try {
            BookieServer bookieServer = serverByIndex(0);
            BookieId bookieId = bookieServer.getBookieId();

            this.bookieClientImpl = (BookieClientImpl) this.bkc.getBookieClient();

            LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32, "pippo".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().setMasterKey(handle.getLedgerMetadata().getLedgerId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));


            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(this.clientConf, bookieClientImpl,
                    bookieId, 1);


            this.bookieClientImpl.channels.put(bookieId, pool);

            PerChannelBookieClient spyInstance = spy(this.bookieClientImpl.create(bookieId, pool,
                    SecurityProviderFactoryFactory.getSecurityProviderFactory(this.clientConf.getTLSProviderFactoryClass()), false));

            doNothing().when(spyInstance).errorOut(isA(PerChannelBookieClient.CompletionKey.class));
            doNothing().when(spyInstance).errorOut(isA(PerChannelBookieClient.CompletionKey.class), isA(int.class));
            doNothing().when(spyInstance).checkTimeoutOnPendingOperations();
            doNothing().when(spyInstance).channelRead(isA(ChannelHandlerContext.class),isA(Object.class));

            if(this.clientConfType.equals(ClientConfType.NOT_WRITABLE_PCBC)) when(spyInstance.isWritable()).thenReturn(false);

            Arrays.fill(pool.clients,spyInstance);

            for (long i = 0; i < this.numberPendingRequestToInsert; i++) {

                ByteBuf toSend = Unpooled.buffer(1024);
                toSend.resetReaderIndex();
                toSend.resetWriterIndex();
                toSend.writeLong(0L);
                toSend.writeLong(0L);
                toSend.writeBytes("Entry content".getBytes(StandardCharsets.UTF_8));
                toSend.writerIndex(toSend.capacity());
                ByteBufList byteBufList = ByteBufList.get(toSend);

                this.bookieClientImpl.addEntry(bookieId,handle.getId(), bookieServer.getBookie().getLedgerStorage().readMasterKey(handle.getId()),
                        i, byteBufList, writeCallback(), new Object() , BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

            }



            if (this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)){
                this.bookieId = bookieId;
            }

            if (this.clientConfType.equals(ClientConfType.CLOSED_CONFIG)){
                this.bookieClientImpl.close();
            }


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                // Bookie Id,            ledger Id,   Client conf type,            Expected Value
                {ParamType.VALID_INSTANCE,    0L,    ClientConfType.STD_CONF,     10L },
                {ParamType.VALID_INSTANCE,   -5L,    ClientConfType.STD_CONF,     10L},
                {ParamType.INVALID_INSTANCE,  0L,    ClientConfType.STD_CONF,     true },
                {ParamType.INVALID_INSTANCE, -5L,    ClientConfType.STD_CONF,     true },
                {ParamType.NULL_INSTANCE,     0L,    ClientConfType.STD_CONF,     true },
                {ParamType.NULL_INSTANCE,     -5L,   ClientConfType.STD_CONF,     true },
                {ParamType.VALID_INSTANCE,     0L,   ClientConfType.CLOSED_CONFIG,  0L},
                {ParamType.VALID_INSTANCE,    -5L,   ClientConfType.CLOSED_CONFIG,  0L},
                {ParamType.VALID_INSTANCE,     0L,   ClientConfType.NOT_WRITABLE_PCBC,  10L | PENDINGREQ_NOTWRITABLE_MASK}
        }) ;
    }


    @Test
    public void test_getNumPendingRequests() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
                try {
                    long actual = this.bookieClientImpl.getNumPendingRequests(this.bookieId, this.ledgerId);
                    Assert.assertEquals(this.expectedNumPendingRequests, actual);
                } catch (Exception e) {
                    Assert.assertTrue("Exception that i expect is raised", (Boolean) this.expectedNumPendingRequests);
                }
        }
    }


    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }

}
