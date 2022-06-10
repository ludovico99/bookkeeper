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

    private static Boolean exceptionInConfigPhase = false;
    private static final ClientConfiguration confLookup = TestBKConfiguration.newClientConfiguration();
    private static BookieClientImpl validConfig;
    private static BookieClientImpl invalidConfig;

    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bookieClient;
    private BookieId bookieId;
    private Object expectedLookupClient;
    private Boolean expectedNullPointerEx = false;
    private Boolean expectedIllegalArgumentException = false;


    public BookieClientImplLookupClientTest(ParamType BookieId, ParamType bookieClient, Object isWritable) {
        super(1);
        configureLookupClient(BookieId, bookieClient, isWritable);


    }

    public static void setValidConfig() throws IOException {

        validConfig = new BookieClientImpl(confLookup, new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

    }

    public static void setInvalidConfig() throws IOException {
        confLookup.setNumChannelsPerBookie(0);

        invalidConfig = new BookieClientImpl(confLookup, new NioEventLoopGroup(),
                UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

    }

    private void configureLookupClient(ParamType bookieId, ParamType bookieClient, Object expected) {
        this.expectedLookupClient = expected;

        switch (bookieId) {
            case VALID_INSTANCE:
                this.bookieId = BookieId.parse("Bookie-1");
                break;

            case INVALID_INSTANCE:
                this.bookieId = BookieId.parse("Bookie-2");
                break;

            case NULL_INSTANCE:
                this.bookieId = null;
                this.expectedNullPointerEx = true;
                break;

        }

        switch (bookieClient) {
            case VALID_CONFIG:
                this.bookieClient = validConfig;
                break;

            case INVALID_CONFIG:
                this.bookieClient = invalidConfig;
                if (bookieId.equals(ParamType.INVALID_INSTANCE))
                    this.expectedIllegalArgumentException = true;
                break;

            case CLOSED_CONFIG:
                validConfig.close();
                this.bookieClient = validConfig;
                break;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        try {
            setValidConfig();

            setInvalidConfig();


            PerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(confLookup, validConfig,
                    BookieId.parse("Bookie-1"), 1);

            validConfig.channels.put(BookieId.parse("Bookie-1"), pool);

            PerChannelBookieClientPool pool2 = new DefaultPerChannelBookieClientPool(confLookup, invalidConfig,
                    BookieId.parse("Bookie-1"), 1);

            invalidConfig.channels.put(BookieId.parse("Bookie-1"), pool2);


            return Arrays.asList(new Object[][]{
                    //BookieId,                  Class Config,              ExpectedValue
                    {ParamType.VALID_INSTANCE, ParamType.VALID_CONFIG,          pool},
                    //{ParamType.INVALID_INSTANCE, ParamType.VALID_CONFIG,        pool},
                    {ParamType.INVALID_INSTANCE, ParamType.CLOSED_CONFIG,        null},
                    {ParamType.VALID_INSTANCE, ParamType.INVALID_CONFIG,        pool2},
                    {ParamType.INVALID_INSTANCE, ParamType.INVALID_CONFIG, new IllegalArgumentException()},
                    {ParamType.NULL_INSTANCE, ParamType.VALID_CONFIG, new NullPointerException()}

            });

        } catch (Exception e) {
            e.printStackTrace();
            exceptionInConfigPhase = true;
        }

        return null;
    }

    @After
    public void tear_down() throws Exception {

        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();

        }
    }

    @AfterClass
    public static void closeAll(){
        validConfig.close();
        invalidConfig.close();
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
