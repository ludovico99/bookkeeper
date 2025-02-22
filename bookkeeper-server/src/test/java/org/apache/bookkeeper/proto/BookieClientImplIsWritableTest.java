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
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;


@RunWith(value = Parameterized.class)
public class BookieClientImplIsWritableTest {

    private Boolean exceptionInConfigPhase = false;

    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bookieClientImpl;
    private BookieId bookieId;
    private long key;
    private Object expectedIsWritable;



    public BookieClientImplIsWritableTest(ParamType BookieId, long key,ClientConfType clientConfType, Object isWritable) {
        configureIsWritable(BookieId, key,clientConfType,isWritable);


    }

    private void configureIsWritable(ParamType enumType,long key,ClientConfType clientConfType, Object expected) {
        this.key = key;
        this.expectedIsWritable = expected;

        try {

            ClientConfiguration confIsWritable = TestBKConfiguration.newClientConfiguration();

            this.bookieClientImpl = new BookieClientImpl(confIsWritable, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            if(clientConfType == ClientConfType.CLOSED_CONFIG) this.bookieClientImpl.close();

            switch (enumType) {
                case VALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(), bookieClientImpl, this.bookieId, 1);
                    pool.clients[0].setWritable((boolean) this.expectedIsWritable);
                    if (clientConfType == ClientConfType.STD_CONF) this.bookieClientImpl.channels.put(this.bookieId, pool);
                    break;

                case INVALID_INSTANCE:
                    this.bookieId = BookieId.parse("");
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    break;

            }


        }
        catch (IllegalArgumentException ie){
            ie.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }


    }



    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //BookieId,                     key,     ClientConf                   value/Expected exception
                {ParamType.VALID_INSTANCE,     -2L,  ClientConfType.STD_CONF,                true},
                {ParamType.VALID_INSTANCE,     -2L,  ClientConfType.STD_CONF,                false},
                {ParamType.INVALID_INSTANCE,   -2L,  ClientConfType.STD_CONF,                new NullPointerException()}, 
                {ParamType.NULL_INSTANCE,      -2L,  ClientConfType.STD_CONF,                new NullPointerException()},

                {ParamType.VALID_INSTANCE,     -2L,  ClientConfType.CLOSED_CONFIG,           true},
                {ParamType.INVALID_INSTANCE,   -2L,  ClientConfType.CLOSED_CONFIG,           new NullPointerException()}


        }) ;
    }


    @Test
    public void test_isWritable() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            try {
                boolean actual;
                actual = this.bookieClientImpl.isWritable(this.bookieId,this.key);
                Assert.assertEquals(this.expectedIsWritable, actual);
            } catch (NullPointerException  | IllegalArgumentException e) {
                Assert.assertEquals("Exception that i expect was raised", this.expectedIsWritable.getClass(), e.getClass());
            }
        }
    }




}
