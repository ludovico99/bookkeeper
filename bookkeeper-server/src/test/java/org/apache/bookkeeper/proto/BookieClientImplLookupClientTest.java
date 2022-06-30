package org.apache.bookkeeper.proto;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.ParamType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;


@RunWith(value = Parameterized.class)
public class BookieClientImplLookupClientTest  {

    private  Boolean exceptionInConfigPhase = false;

    //Test public PerChannelBookieClientPool lookupClient(BookieId addr)
    private BookieClientImpl bookieClientImpl;
    private BookieId bookieId;
    private Object expectedLookupClient;


    public BookieClientImplLookupClientTest(ParamType BookieId, ClientConfType bookieClient) {
        configureLookupClient(BookieId, bookieClient);


    }

    private void configureLookupClient(ParamType bookieId, ClientConfType bookieClient) {
       
        
        try {
            PerChannelBookieClientPool pool = null;
            PerChannelBookieClientPool pool2 = null;
            
            switch (bookieClient){
                case STD_CONF:
                case CLOSED_CONFIG:
                    ClientConfiguration confLookupValid =TestBKConfiguration.newClientConfiguration();
                    bookieClientImpl= new BookieClientImpl(confLookupValid, new NioEventLoopGroup(),
                            UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                            new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                            BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

                    pool = new DefaultPerChannelBookieClientPool(confLookupValid, bookieClientImpl,
                            BookieId.parse("Bookie-1"), 1);

                    bookieClientImpl.channels.put(BookieId.parse("Bookie-1"), pool);
                    break;
                case INVALID_CONFIG:
                    ClientConfiguration confLookupInvalid =TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(0);
                    
                   this.bookieClientImpl = new BookieClientImpl(confLookupInvalid, new NioEventLoopGroup(),
                            UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                            new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                            BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

                   pool2 = new DefaultPerChannelBookieClientPool(confLookupInvalid, bookieClientImpl,
                            BookieId.parse("Bookie-1"), 1);

                    bookieClientImpl.channels.put(BookieId.parse("Bookie-1"), pool2);
                    break;
                    
            }
            
            

            switch (bookieId) {
                case VALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    switch (bookieClient) {
                        case STD_CONF:
                            this.expectedLookupClient = pool;
                            break;
                        case INVALID_CONFIG:
                            this.expectedLookupClient = pool2;
                            break;
                        case CLOSED_CONFIG:
                            this.expectedLookupClient = null;
                            this.bookieClientImpl.close();
                            break;
                    }
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-2");
                    switch (bookieClient) {
                        case STD_CONF:
                            this.expectedLookupClient = Boolean.FALSE;
                            break;
                        case INVALID_CONFIG:
                            this.expectedLookupClient = new IllegalArgumentException();
                            break;
                        case CLOSED_CONFIG:
                            this.expectedLookupClient = null;
                            this.bookieClientImpl.close();
                            break;
                    }
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedLookupClient = new NullPointerException();
                    switch (bookieClient) {
                        case STD_CONF:
                        case INVALID_CONFIG:
                            break;
                        case CLOSED_CONFIG:
                            this.bookieClientImpl.close();
                    }
                    break;
            }


        }catch (Exception e){

            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //BookieId,                  Class Config,
                {ParamType.VALID_INSTANCE,   ClientConfType.STD_CONF},
                {ParamType.VALID_INSTANCE,   ClientConfType.INVALID_CONFIG},
                {ParamType.VALID_INSTANCE,   ClientConfType.CLOSED_CONFIG},

                {ParamType.INVALID_INSTANCE, ClientConfType.STD_CONF},
                {ParamType.INVALID_INSTANCE, ClientConfType.INVALID_CONFIG},
                {ParamType.INVALID_INSTANCE, ClientConfType.CLOSED_CONFIG},

                {ParamType.NULL_INSTANCE,    ClientConfType.STD_CONF},
                {ParamType.NULL_INSTANCE,    ClientConfType.INVALID_CONFIG},
                {ParamType.NULL_INSTANCE,    ClientConfType.CLOSED_CONFIG}

        });
    }





    @Test
    public void test_LookupClient() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {
                PerChannelBookieClientPool client = this.bookieClientImpl.lookupClient(this.bookieId);
                if(this.expectedLookupClient instanceof Boolean) Assert.assertFalse((Boolean) this.expectedLookupClient);
                else Assert.assertEquals("Expected instance", this.expectedLookupClient, client);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertEquals("Exception that I expect was raised", this.expectedLookupClient.getClass(), e.getClass());
            }
        }
    }
}