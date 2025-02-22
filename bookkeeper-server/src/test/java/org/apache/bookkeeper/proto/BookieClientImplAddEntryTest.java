package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.Counter;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;

import java.nio.charset.StandardCharsets;
import java.util.*;



import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplAddEntryTest extends BookKeeperClusterTestCase {

    private  BookieClientImpl bookieClientImpl;


    //Test:   void addEntry(final BookieId addr,
    //                         final long ledgerId,
    //                         final byte[] masterKey,
    //                         final long entryId,
    //                         final ByteBufList toSend,
    //                         final WriteCallback cb,
    //                         final Object ctx,
    //                         final int options,
    //                         final boolean allowFastFail,
    //                         final EnumSet<WriteFlag> writeFlags)
    private BookkeeperInternalCallbacks.WriteCallback writeCallback;
    private int flags;
    private Object ctx;
    private Object expectedAdd;
    private long ledgerId;
    private ByteBufList toSend;
    private byte[] ms;
    private ParamType msParamType;
    private ParamType bookieIdParamType;
    private ClientConfType clientConfTypeEnum;
    private BookieId bookieId;
    private long entryId;
    private boolean allowFastFail;
    private EnumSet<WriteFlag> writeFlags;


    public BookieClientImplAddEntryTest(ParamType bookieId, long ledgerId, ParamType ms, long entryId, ByteBufList toSend, ParamType cb, Object ctx, int flags, boolean allowFastFail, ParamType wf, ClientConfType clientConfType, Object expectedAdd) {
        super(3);
        configureAdd(bookieId, ledgerId, ms, entryId, toSend, cb, ctx, flags, allowFastFail, wf, clientConfType, expectedAdd);

    }


    private void configureAdd(ParamType bookieId, long ledgerId, ParamType ms, long entryId,ByteBufList toSend, ParamType cb, Object ctx, int flags, boolean allowFastFail, ParamType wf, ClientConfType clientConfType, Object expectedAdd) {

        this.bookieIdParamType = bookieId;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.clientConfTypeEnum = clientConfType;
        this.ctx = ctx;
        this.flags = flags;
        this.expectedAdd = expectedAdd;
        this.msParamType = ms;
        this.toSend = toSend;
        this.allowFastFail = allowFastFail;



        try {

           this.setBaseClientConf(TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(1));

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
                    this.writeCallback = writeCallback();
                    break;

                case NULL_INSTANCE:
                    this.writeCallback = null;
                    break;

                case INVALID_INSTANCE:
                    //to be determined
                    break;
            }

            switch (ms){
                case VALID_INSTANCE:
                    break;
                case INVALID_INSTANCE:
                    this.ms = "".getBytes(StandardCharsets.UTF_8);
                    break;

                case NULL_INSTANCE:
                    this.ms = null;
                    break;

            }

            switch (wf){
                case VALID_INSTANCE:
                    this.writeFlags = EnumSet.allOf(WriteFlag.class);
                    break;
                case NULL_INSTANCE:
                    this.writeFlags = null;
                    break;
                case INVALID_INSTANCE:
                    this.writeFlags = EnumSet.complementOf(EnumSet.allOf(WriteFlag.class));
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

            this.bookieClientImpl = (BookieClientImpl) this.bkc.getBookieClient();


            LedgerHandle handle = this.bkc.createLedger(BookKeeper.DigestType.CRC32,"test".getBytes(StandardCharsets.UTF_8));

            bookieServer.getBookie().getLedgerStorage().
                    setMasterKey(handle.getId(),
                            "masterKey".getBytes(StandardCharsets.UTF_8));


            if(this.bookieIdParamType == ParamType.VALID_INSTANCE)     this.bookieId = bookieId;
            if(this.msParamType == ParamType.VALID_INSTANCE)  this.ms = bookieServer.getBookie().getLedgerStorage().readMasterKey(handle.getId());

            switch (clientConfTypeEnum){
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
                    this.bkc.getMainWorkerPool().shutdown();
                    break;
            }

        }catch (Exception e){
            e.printStackTrace();
           this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        ByteBuf toSend = Unpooled.buffer(1024);
        toSend.resetReaderIndex();
        toSend.resetWriterIndex();
        toSend.writeLong(0L);
        toSend.writeLong(-5L);
        toSend.writeBytes("Entry content".getBytes(StandardCharsets.UTF_8));
        toSend.writerIndex(toSend.capacity());
        ByteBufList byteBufList = ByteBufList.get(toSend);

        ByteBufList emptyByteBufList = ByteBufList.get(Unpooled.EMPTY_BUFFER);


        return Arrays.asList(new Object[][]{
                //Bookie_ID                   Ledger_id   Master key             Entry_id   toSend,         ReadEntryCallback,         Object          Flags                AllowFastFail  WriteFlags               ClientConf                     Raise exception or RC
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.OK},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, true,  ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.OK},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.NULL_INSTANCE,    ClientConfType.STD_CONF,        -1}, //Mean no callback invoked
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.INVALID_INSTANCE, ClientConfType.STD_CONF,        BKException.Code.OK},

                {  ParamType.INVALID_INSTANCE,    0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.BookieHandleNotAvailableException},
                {  ParamType.VALID_INSTANCE,     -5L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.OK}, //Andrebbe gestito meglio
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,  -5L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.OK},
                {  ParamType.NULL_INSTANCE,       0L,   ParamType.VALID_INSTANCE,  -5L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter() , BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        true},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   null,             ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        true},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   emptyByteBufList, ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.WriteException},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.INVALID_INSTANCE, 0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF,        BKException.Code.OK},
                                                                                                                                                                                    
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.CLOSED_CONFIG,   BKException.Code.ClientClosedException},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.INVALID_CONFIG,  BKException.Code.ClientClosedException},
                {  ParamType.VALID_INSTANCE,      0L,   ParamType.VALID_INSTANCE,   0L,   byteBufList,      ParamType.VALID_INSTANCE,  new Counter(),  BookieProtocol.ADDENTRY, false, ParamType.VALID_INSTANCE,   ClientConfType.REJECT_CONFIG,   BKException.Code.InterruptedException},



        }) ;
    }


    @Test
    public void test_Add() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {

                if (this.writeFlags == null) verifyNoInteractions(this.writeCallback);
                else {

                    ((Counter) this.ctx).inc();

                    this.bookieClientImpl.addEntry(this.bookieId, this.ledgerId, this.ms,
                            this.entryId, this.toSend, this.writeCallback, this.ctx, this.flags, this.allowFastFail, this.writeFlags);

                    ((Counter) this.ctx).wait(0);

                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.writeCallback).writeComplete(argument.capture(), nullable(Long.class), nullable(Long.class),
                            nullable(BookieId.class), isA(Object.class));
                    Assert.assertEquals(this.expectedAdd, argument.getValue());
                }

            } catch (Exception e){
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedAdd);
            }

        }
    }


    private BookkeeperInternalCallbacks.WriteCallback writeCallback(){

        return spy(new BookkeeperInternalCallbacks.WriteCallback() {
                       @Override
                       public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                           Counter counter = (Counter) ctx;
                           counter.dec();
                           System.out.println("WRITE: rc = " + rc + " for entry: " + entryId + " at ledger: " +
                                   ledgerId + " at bookie: " + addr );
                       }
                   }
        );
    }

}
