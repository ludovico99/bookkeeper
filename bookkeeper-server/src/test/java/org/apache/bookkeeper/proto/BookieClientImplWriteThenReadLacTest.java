package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class BookieClientImplWriteThenReadLacTest extends BookKeeperClusterTestCase {

    private static Boolean exceptionInConfigPhase = false;


    //Test: readLac(final BookieId addr, final long ledgerId, final ReadLacCallback cb,
    //            final Object ctx)
    private BookkeeperInternalCallbacks.ReadLacCallback readLacCallback;
    private  BookieClient bookieClientImpl;
    private Object ctx;
    private Object expectedReadLac;
    private Long ledgerId;
    private ParamType bookieIdParamType;
    private ParamType ledgerIdParamType;
    private BookieId bookieId;



    public BookieClientImplWriteThenReadLacTest(ParamType bookieId, ParamType ledgerId , ParamType cb, Object ctx, Object expectedWriteLac) {
        super(3);
        configureReadLac(bookieId, ledgerId, cb, ctx, expectedWriteLac);
    }

    private void configureReadLac(ParamType bookieId, ParamType ledgerId, ParamType cb, Object ctx, Object expectedReadLac) {

        this.bookieIdParamType = bookieId;
        this.ledgerIdParamType = ledgerId;
        this.ctx = ctx;
        this.expectedReadLac = expectedReadLac;

        switch (bookieId){
            case VALID_INSTANCE:
                break;

            case NULL_INSTANCE:
                this.bookieId = null;
                break;

            case INVALID_INSTANCE:
                this.bookieId = BookieId.parse("Bookie-1");
                break;

        }

        switch (ledgerId) {
            case VALID_INSTANCE:
                break;

            case NULL_INSTANCE:
                this.ledgerId = null;
                break;

            case INVALID_INSTANCE:
                this.ledgerId = -1L;
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



    @Before
    public void set_up() {

        this.bookieClientImpl = bkc.getBookieClient();

        try {
            BookieServer bookieServer = serverByIndex(0);
            bookieServer.getBookie().getLedgerStorage().setMasterKey(0,
                    "masterKey".getBytes(StandardCharsets.UTF_8));



           LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32,"pippo".getBytes(StandardCharsets.UTF_8));

           handle.addEntry("This is the entry content".getBytes(StandardCharsets.UTF_8));

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)){
                this.bookieId = serverByIndex(0).getBookieId();
                this.ledgerId = handle.getId();
            }

            bookieClientImpl.writeLac(serverByIndex(0).getBookieId(), handle.getId(), "masterKey".getBytes(StandardCharsets.UTF_8),
                    handle.readLastAddConfirmed(),
                    ByteBufList.get(Unpooled.buffer("This is the entry content".getBytes(StandardCharsets.UTF_8).length)), mockWriteLacCallback(), this.ctx);

            while(bookieClientImpl.getNumPendingRequests(serverByIndex(0).getBookieId(), handle.getId()) != 0) {
                System.out.println("Write not completed yet");
            }

            if(this.bookieIdParamType.equals(ParamType.CLOSED_CONFIG)) this.bookieClientImpl.close();

        }catch (Exception e){
            exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        //ByteBuf byteBuf = Unpooled.buffer("This is the entry content".getBytes(StandardCharsets.UTF_8).length);

        //boolean interactions = false; //For invalid istances callback won't be invoked

        return Arrays.asList(new Object[][]{
                //Bookie_ID                         Led_ID                          ReadLacCallback                   ctx        RaiseException
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  new Object(),       0},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.NULL_INSTANCE,  new Object(),        true},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.NULL_INSTANCE,  new Object(),        true},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.NULL_INSTANCE,  new Object(),        true},
              //  { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE, new Object(),         -13},
                { ParamType.INVALID_INSTANCE,    ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.CLOSED_CONFIG,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true},
                { ParamType.CLOSED_CONFIG,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE, new Object(),        true}
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
    public void test_ReadLac() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {
                bookieClientImpl.readLac(this.bookieId, this.ledgerId, this.readLacCallback, this.ctx);

                while(bookieClientImpl.getNumPendingRequests(this.bookieId,this.ledgerId) != 0){
                    System.out.println("Waiting");
                }

                if(this.bookieIdParamType.equals(ParamType.INVALID_INSTANCE)) verifyNoInteractions(this.readLacCallback);
                else{
                    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(int.class);
                    verify(this.readLacCallback).readLacComplete(argument.capture(), isA(Long.class), isA(ByteBuf.class), isA(ByteBuf.class), isA(Object.class));
                    Assert.assertEquals(this.expectedReadLac, argument.getValue());
                }
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



}
