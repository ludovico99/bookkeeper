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
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplAddThenReadEntryTest extends BookKeeperClusterTestCase {

    private static Boolean exceptionInConfigPhase = false;
    private static ClientConfiguration clientConf;
    private static BookieClientImpl bookieClientImpl;


    //Test:   readEntry(BookieId addr, long ledgerId, long entryId,
    //                          ReadEntryCallback cb, Object ctx, int flags)
    private BookkeeperInternalCallbacks.ReadEntryCallback readCallback;
    private int flags;
    private Object ctx;
    private Object expectedRead;
    private Long ledgerId;
    private ParamType ledgerIdParamType;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private Long entryId;




    public BookieClientImplAddThenReadEntryTest(ParamType bookieId, ParamType ledgerId , ParamType entryId, ParamType cb, Object ctx, int flags, Object expectedReadLac) {
        super(3);
        configureAddThenRead(bookieId, ledgerId, entryId, cb, ctx, flags, expectedReadLac);

    }

    private static void setBookieClientImpl() throws IOException {
        clientConf = TestBKConfiguration.newClientConfiguration();
        bookieClientImpl = new BookieClientImpl(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1), new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
    }

    private void configureAddThenRead(ParamType bookieId, ParamType ledgerId, ParamType entryId, ParamType cb, Object ctx, int flags, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.ctx = ctx;
        this.flags = flags;
        this.expectedRead = expectedReadLac;
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

        switch (entryId){
            case VALID_INSTANCE:
                this.entryId = 0L;
                break;

            case NULL_INSTANCE:
                this.entryId = null;
                break;

            case INVALID_INSTANCE:
                this.entryId = -5L;
                break;

        }

        switch (cb) {
            case VALID_INSTANCE:
                this.readCallback = mockReadCallback();
                break;

            case NULL_INSTANCE:
                this.readCallback = null;
                break;

            case INVALID_INSTANCE:
                //to be determined
                break;
        }

    }



    @Before
    public void set_Up() {

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

            if(bookieIdParamType.equals(ParamType.VALID_INSTANCE)){
                this.bookieId = bookieServer.getBookieId();
            }
            if(ledgerIdParamType.equals(ParamType.VALID_INSTANCE)){
                this.ledgerId = handle.getLedgerMetadata().getLedgerId();
            }


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
                    //Bookie_ID                   Ledger_id                   Entry_id                ReadEntryCallback          Object               Flags                      Raise exception
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,    ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     BKException.Code.OK},
                {  ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,    ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     true},
                {  ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,  ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     BKException.Code.NoSuchEntryException},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,    ParamType.INVALID_INSTANCE,   ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     true},
                {  ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,    ParamType.INVALID_INSTANCE,   ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     true},
                {  ParamType.VALID_INSTANCE,      ParamType.NULL_INSTANCE,     ParamType.VALID_INSTANCE,     ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     true},
                {  ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,    ParamType.NULL_INSTANCE,      ParamType.VALID_INSTANCE,  new Object() , BookieProtocol.READENTRY,     true}

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
    public void test_ReadAfterAdd() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {

                bookieClientImpl.readEntry(this.bookieId, this.ledgerId,this.entryId, this.readCallback,this.ctx,this.flags,"masterKey".getBytes(StandardCharsets.UTF_8));

                while(bookieClientImpl.getNumPendingRequests(this.bookieId,this.ledgerId) != 0){
                    // System.out.println("Waiting reading completion");
                }

                ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.readCallback).readEntryComplete(argument.capture(), isA(long.class), isA(long.class),
                            isA(ByteBuf.class), isA(Object.class));
                    Assert.assertEquals(this.expectedRead, argument.getValue());

                 //Assert.assertFalse("No raising exception", (Boolean) this.expectedRead);

            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedRead);
            }

        }
    }


    public BookkeeperInternalCallbacks.WriteCallback mockWriteCallback(){
        BookkeeperInternalCallbacks.WriteCallback cb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        doNothing().when(cb).writeComplete(isA(Integer.class),isA(long.class),isA(long.class),isA(BookieId.class),
                isA(Object.class));

        return cb;
    }

    public BookkeeperInternalCallbacks.ReadEntryCallback mockReadCallback(){
        BookkeeperInternalCallbacks.ReadEntryCallback cb = mock(BookkeeperInternalCallbacks.ReadEntryCallback.class);
        doNothing().when(cb).readEntryComplete(isA(Integer.class), isA(long.class),  isA(long.class),
                isA(ByteBuf.class), isA(Object.class));

        return cb;
    }

    public BookkeeperInternalCallbacks.WriteLacCallback mockWriteLacCallback(){
        BookkeeperInternalCallbacks.WriteLacCallback cb = mock(BookkeeperInternalCallbacks.WriteLacCallback.class);
        doNothing().when(cb).writeLacComplete(isA(Integer.class),isA(long.class),isA(BookieId.class),
                isA(Object.class));

        return cb;
    }


}
