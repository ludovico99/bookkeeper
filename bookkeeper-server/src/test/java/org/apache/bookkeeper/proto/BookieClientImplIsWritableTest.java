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
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;



@RunWith(value = Parameterized.class)
public class BookieClientImplIsWritableTest extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;


    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bcIsWritable;
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
                case INVALID_INSTANCE:
                    this.expectedIsWritable = expected;
                    break;

                case INVALID_CONFIG:
                    confIsWritable.setNumChannelsPerBookie(0);
                    this.expectedIllegalArgumentException = true;
                    this.expectedIsWritable = expected;
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedIsWritable = expected;
                    this.expectedNullPointerEx = true;
                    break;

            }

            this.bcIsWritable = new BookieClientImpl(confIsWritable, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            if (enumType.equals(ParamType.CLOSED_CONFIG)) {
                    this.bcIsWritable.close();
                    this.expectedIsWritable = expected;
            }

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = false;
        }

    }



    @Before
    public void set_up() {

        try {

            if (!bookieIdParamType.equals(ParamType.NULL_INSTANCE)) this.bookieId = serverByIndex(0).getBookieId();


            if (bookieIdParamType.equals(ParamType.INVALID_INSTANCE)) {

                DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(), bcIsWritable, this.bookieId, 1);
                pool.clients[0].setWritable(false);

                bcIsWritable.channels.put(this.bookieId, pool);
            }

            if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)) {


                DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(), bcIsWritable, this.bookieId, 1);

                bcIsWritable.channels.put(this.bookieId, pool);

            }
        }catch (Exception e){
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //BookieId,                     key,  ExpectedValue
                {ParamType.VALID_INSTANCE,     -1L,         true},
                {ParamType.INVALID_INSTANCE,   -1L,         false},
                {ParamType.CLOSED_CONFIG,      -1L,         true},
                {ParamType.INVALID_CONFIG,      2L,         new IllegalArgumentException()},
                {ParamType.NULL_INSTANCE,       1L,         new NullPointerException()}

        }) ;
    }

    @After
    public void tear_down() throws Exception {

        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();

        }
    }


    @Test
    public void test_isWritable() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);

        else {
            if (expectedNullPointerEx || expectedIllegalArgumentException) {
                try {
                    bcIsWritable.isWritable(this.bookieId, this.key);
                    Assert.fail("Test case failed");
                } catch (NullPointerException  | IllegalArgumentException e) {
                    Assert.assertEquals("Exception that i expect was raised", this.expectedIsWritable.getClass(), e.getClass());
                }

            } else Assert.assertEquals(this.expectedIsWritable, bcIsWritable.isWritable(this.bookieId, this.key));

        }
    }




}
