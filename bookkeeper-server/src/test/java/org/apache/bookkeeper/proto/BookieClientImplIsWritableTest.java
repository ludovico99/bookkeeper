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
import org.apache.bookkeeper.util.ClientConfType;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;


@RunWith(value = Parameterized.class)
public class BookieClientImplIsWritableTest extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;

    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bookieClientImpl;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private long key;
    private Object expectedIsWritable;
    private Boolean expectedNullPointerEx = false;
    private Boolean expectedIllegalArgumentException = false;


    public BookieClientImplIsWritableTest(ParamType BookieId, long key, Object isWritable) {
        super(1);

        configureIsWritable(BookieId, key,isWritable);


    }

    private void configureIsWritable(ParamType enumType,long key, Object expected) {
        this.key = key;
        this.expectedIsWritable = expected;
        this.bookieIdParamType = enumType;

        try {

            ClientConfiguration confIsWritable = TestBKConfiguration.newClientConfiguration();

            switch (enumType) {
                case VALID_INSTANCE:
                    break;
                case INVALID_INSTANCE:
                    this.expectedIllegalArgumentException = true;
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedNullPointerEx = true;
                    break;

            }

            this.bookieClientImpl = new BookieClientImpl(confIsWritable, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

//            switch (clientConfType) {
//                case STD_CONF:
//                    break;
//
//                case CLOSED_CONFIG:
//                    this.bookieClientImpl.close();
//                    break;
//
//                case INVALID_CONFIG:
//                    confIsWritable.setNumChannelsPerBookie(0);
//                    this.expectedIllegalArgumentException = true;
//                    break;
//
//            }
        }
        catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }

    }



    @Before
    public void set_up() {

        try {

            if (this.bookieIdParamType.equals(ParamType.VALID_INSTANCE)) {
                this.bookieId = serverByIndex(0).getBookieId();
                DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(), bookieClientImpl, this.bookieId, 1);
                pool.clients[0].setWritable((boolean) this.expectedIsWritable);
                this.bookieClientImpl.channels.put(this.bookieId, pool);

            }



        }catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //BookieId,                     key,     ClientConf                   value/Expected exception
                {ParamType.VALID_INSTANCE,     -2L, true},
                {ParamType.VALID_INSTANCE,     -2L, false},
                {ParamType.INVALID_INSTANCE,   -2L, new IllegalArgumentException()},
                {ParamType.NULL_INSTANCE,       1L, new NullPointerException()}
//                {ParamType.VALID_INSTANCE,     -1L, ClientConfType.STD_CONF,                true},
//                {ParamType.VALID_INSTANCE,     -1L, ClientConfType.STD_CONF,                false},
//                {ParamType.INVALID_INSTANCE,   -1L, ClientConfType.STD_CONF,                new IllegalArgumentException()},
//                {ParamType.VALID_INSTANCE,     -1L, ClientConfType.CLOSED_CONFIG,           true},
//                {ParamType.INVALID_INSTANCE,    2L, ClientConfType.CLOSED_CONFIG,           new IllegalArgumentException()},
//                {ParamType.NULL_INSTANCE,       1L, ClientConfType.STD_CONF,                new NullPointerException()}

        }) ;
    }


    @Test
    public void test_isWritable() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            if (this.expectedNullPointerEx || this.expectedIllegalArgumentException) {
                try {
                    if(this.bookieIdParamType.equals(ParamType.INVALID_INSTANCE)) this.bookieClientImpl.isWritable(BookieId.parse(""), this.key);
                    else this.bookieClientImpl.isWritable(this.bookieId, this.key);
                    Assert.fail("Test case failed");
                } catch (NullPointerException  | IllegalArgumentException e) {
                    Assert.assertEquals("Exception that i expect was raised", this.expectedIsWritable.getClass(), e.getClass());
                }

            } else Assert.assertEquals(this.expectedIsWritable, this.bookieClientImpl.isWritable(this.bookieId, this.key));

        }
    }




}
