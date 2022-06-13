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
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ParamType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;



@RunWith(value = Parameterized.class)
public class BookieClientImplLookupClientTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  ClientConfiguration confLookup;

    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bookieClient;
    private BookieId bookieId;
    private Object expectedLookupClient;
    private Boolean expectedNullPointerEx = false;
    private Boolean expectedIllegalArgumentException = false;


    public BookieClientImplLookupClientTest(ParamType BookieId, ParamType bookieClient) {
        super(1);
        configureLookupClient(BookieId, bookieClient);


    }

    private void configureLookupClient(ParamType bookieId, ParamType bookieClient) {

       try {
           ClientConfiguration confLookupValid=TestBKConfiguration.newClientConfiguration();
           ClientConfiguration confLookupInvalid=TestBKConfiguration.newClientConfiguration().setNumChannelsPerBookie(0);

           BookieClientImpl validConfig = new BookieClientImpl(confLookupValid, new NioEventLoopGroup(),
                   UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                   new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                   BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

           BookieClientImpl invalidConfig = new BookieClientImpl(confLookupInvalid, new NioEventLoopGroup(),
                   UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                   new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                   BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

           PerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(confLookupValid, validConfig,
                   BookieId.parse("Bookie-1"), 1);

           validConfig.channels.put(BookieId.parse("Bookie-1"), pool);

           PerChannelBookieClientPool pool2 = new DefaultPerChannelBookieClientPool(confLookupInvalid, invalidConfig,
                   BookieId.parse("Bookie-1"), 1);

           invalidConfig.channels.put(BookieId.parse("Bookie-1"), pool2);

           switch (bookieId) {
               case VALID_INSTANCE:
                   this.bookieId = BookieId.parse("Bookie-1");
                   switch (bookieClient) {
                       case VALID_CONFIG:
                           this.expectedLookupClient = pool;
                           this.bookieClient = validConfig;
                           break;
                       case INVALID_CONFIG:
                           this.expectedLookupClient = pool2;
                           this.bookieClient = invalidConfig;
                           break;
                       case CLOSED_CONFIG:
                           this.expectedLookupClient = null;
                           validConfig.close();
                           this.bookieClient = validConfig;
                   }
                   break;

               case INVALID_INSTANCE:
                   this.bookieId = BookieId.parse("Bookie-2");
                   switch (bookieClient) {
                       case VALID_CONFIG:
                           this.expectedLookupClient = new DefaultPerChannelBookieClientPool(confLookupValid, validConfig,
                                   BookieId.parse("Bookie-2"), 1);
                           this.bookieClient = validConfig;
                           break;
                       case INVALID_CONFIG:
                           this.expectedLookupClient = new IllegalArgumentException();
                           this.expectedIllegalArgumentException = true;
                           this.bookieClient = invalidConfig;
                           break;
                       case CLOSED_CONFIG:
                           this.expectedLookupClient = null;
                           validConfig.close();
                           this.bookieClient = validConfig;
                           break;
                   }

               case NULL_INSTANCE:
                   this.bookieId = null;
                   this.expectedNullPointerEx = true;
                   this.expectedLookupClient = new NullPointerException();
                   switch (bookieClient) {
                       case VALID_CONFIG:
                           this.bookieClient = validConfig;
                           break;
                       case INVALID_CONFIG:
                           this.bookieClient = invalidConfig;
                           break;
                       case CLOSED_CONFIG:
                           validConfig.close();
                           this.bookieClient = validConfig;
                   }
                   break;
           }


       }catch (Exception e){
           e.printStackTrace();
           //this.exceptionInConfigPhase = true;
       }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //BookieId,                  Class Config,
                {ParamType.VALID_INSTANCE, ParamType.VALID_CONFIG},
                {ParamType.VALID_INSTANCE, ParamType.INVALID_CONFIG},
                {ParamType.VALID_INSTANCE, ParamType.CLOSED_CONFIG},
                {ParamType.INVALID_INSTANCE, ParamType.VALID_CONFIG},
                {ParamType.INVALID_INSTANCE, ParamType.INVALID_CONFIG},
                {ParamType.INVALID_INSTANCE, ParamType.CLOSED_CONFIG},
                {ParamType.NULL_INSTANCE, ParamType.VALID_CONFIG},
                {ParamType.NULL_INSTANCE, ParamType.INVALID_CONFIG},
                {ParamType.NULL_INSTANCE, ParamType.CLOSED_CONFIG}




        });
    }




    @After
    public void tear_down() throws Exception {

        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();

        }
    }



    @Test
    public void test_LookupClient() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);

        else {
            if (expectedNullPointerEx || expectedIllegalArgumentException) {
                try {
                    this.bookieClient.lookupClient(this.bookieId);
                } catch (NullPointerException  | IllegalArgumentException e) {
                    Assert.assertEquals("Exception that I expect was raised", this.expectedLookupClient.getClass(), e.getClass());
                }

            } else Assert.assertEquals(this.expectedLookupClient, this.bookieClient.lookupClient(this.bookieId));

        }
    }




}
