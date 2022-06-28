package org.apache.bookkeeper.proto;

import com.google.protobuf.ExtensionRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.WriteFlag;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.bookkeeper.proto.BookieClient.PENDINGREQ_NOTWRITABLE_MASK;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@RunWith(value = Parameterized.class)
public class BookieClientImplGetNumPendingRequestsTest extends BookKeeperClusterTestCase {

    private  BookieClientImpl bookieClientImpl;

    //Test: getNumPendingRequests(BookieId address, long ledgerId)
    private Object expectedNumPendingRequests;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfType;
    private BookieId bookieId;
    private int numberPendingRequestToInsert;



    public BookieClientImplGetNumPendingRequestsTest(ParamType bookieId, long ledgerId, ClientConfType clientConfType,int request, Object expected ) {
        super(3);
        configureGetNumPendingRequests(bookieId, ledgerId,clientConfType, request, expected);
    }



    private void configureGetNumPendingRequests(ParamType bookieId, long ledgerId,ClientConfType clientConfType, int request, Object expected) {

        this.bookieIdParamType = bookieId;
        this.clientConfType = clientConfType;
        this.ledgerId = ledgerId;
        this.numberPendingRequestToInsert = request;
        this.expectedNumPendingRequests = expected;

        try {

            this.baseClientConf = TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1);

            switch (bookieId) {
                case VALID_INSTANCE:
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("");
                    break;
            }
        }
        catch (IllegalArgumentException ie){
            ie.printStackTrace();
            this.expectedNumPendingRequests = true;
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

            LedgerHandle handle = this.bkc.createLedger(BookKeeper.DigestType.CRC32, "test".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().setMasterKey(handle.getId(),
                    "masterKey".getBytes(StandardCharsets.UTF_8));


            DefaultPerChannelBookieClientPool pool = spy(new DefaultPerChannelBookieClientPool(this.baseClientConf, this.bookieClientImpl,
                    bookieId, 1));


            this.bookieClientImpl.channels.put(bookieId, pool);

            PerChannelBookieClient perChannelBookieClient= spy(this.bookieClientImpl.create(bookieId, pool,
                    SecurityProviderFactoryFactory.getSecurityProviderFactory(this.baseClientConf.getTLSProviderFactoryClass()), false));


            doNothing().when(perChannelBookieClient).errorOut(isA(PerChannelBookieClient.CompletionKey.class));
            doNothing().when(perChannelBookieClient).errorOut(isA(PerChannelBookieClient.CompletionKey.class), isA(int.class));
            doNothing().when(perChannelBookieClient).errorOutOutstandingEntries(isA(int.class));
            doNothing().when(perChannelBookieClient).checkTimeoutOnPendingOperations();
            doNothing().when(perChannelBookieClient).channelRead(isA(ChannelHandlerContext.class),isA(Object.class));

            Arrays.fill(pool.clients,perChannelBookieClient);

            if(this.clientConfType == ClientConfType.NOT_WRITABLE_PCBC) when(perChannelBookieClient.isWritable()).thenReturn(false);

            Counter counter = new Counter();

            for (int i = 0; i < this.numberPendingRequestToInsert; i++) {
                counter.inc();

                ByteBuf toSend = Unpooled.buffer(1024);
                toSend.resetReaderIndex();
                toSend.resetWriterIndex();
                toSend.writeLong(0L);
                toSend.writeLong(0L);
                toSend.writeBytes("Entry content".getBytes(StandardCharsets.UTF_8));
                toSend.writerIndex(toSend.capacity());
                ByteBufList byteBufList = ByteBufList.get(toSend);

                this.bookieClientImpl.addEntry(bookieId,handle.getId(), bookieServer.getBookie().getLedgerStorage().readMasterKey(handle.getId()),
                        i, byteBufList, writeCallback(), counter , BookieProtocol.ADDENTRY, false, EnumSet.allOf(WriteFlag.class));

            }

            if (this.bookieIdParamType == ParamType.VALID_INSTANCE) this.bookieId = bookieId;

            if (this.clientConfType == ClientConfType.CLOSED_CONFIG) this.bookieClientImpl.close();




        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                
                // Bookie Id,            ledger Id,   Client conf type,          Request to insert, expected Value
                {ParamType.VALID_INSTANCE,    0L,   ClientConfType.CLOSED_CONFIG,    20,              0L},
                {ParamType.VALID_INSTANCE,    -5L,  ClientConfType.CLOSED_CONFIG,    20,              0L},
                {ParamType.VALID_INSTANCE,    0L,   ClientConfType.STD_CONF,         20,              20L},
                {ParamType.VALID_INSTANCE,   -5L,   ClientConfType.STD_CONF,         20,              20L},
                {ParamType.INVALID_INSTANCE,  0L,   ClientConfType.STD_CONF,         20,              true}, //Illegal Argument exception
                {ParamType.INVALID_INSTANCE, -5L,   ClientConfType.STD_CONF,         20,              true}, //Illegal Argument exception
                {ParamType.NULL_INSTANCE,     0L,   ClientConfType.STD_CONF,         20,              true}, //Null pointer exception
                {ParamType.NULL_INSTANCE,    -5L,   ClientConfType.STD_CONF,         20,              true}, //Null pointer exception
                {ParamType.VALID_INSTANCE,     0L,  ClientConfType.NOT_WRITABLE_PCBC,20,              20 |  PENDINGREQ_NOTWRITABLE_MASK}
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
                    if (this.expectedNumPendingRequests instanceof Boolean)
                    Assert.assertTrue("Exception that i expect is raised", (Boolean) this.expectedNumPendingRequests);
                    else Assert.fail("test case failed");
                }
        }
    }


    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return (rc, ledger, entry, addr, ctx1) -> {
            ((Counter) ctx1).dec();
            System.out.println("WRITE: rc = " + rc + " for entry: " + entry + " at ledger: " +
                    ledger + " at bookie: " + addr );

        };
    }

}
