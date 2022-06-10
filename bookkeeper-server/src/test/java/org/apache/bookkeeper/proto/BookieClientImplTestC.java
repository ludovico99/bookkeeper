package org.apache.bookkeeper.proto;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;



@RunWith(value = Parameterized.class)
public class BookieClientImplTestC extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;


    //Test: isWritable(BookieId address, long key)
    private BookieClientImpl bcIsWritable;
    private ParamType bookieIdParamType;
    private BookieId bookieId;
    private long key;
    private Object expectedIsWritable;
    private Boolean expectedNullPointerEx = false;
    private Boolean expectedIllegalArgumentException = false;


    public BookieClientImplTestC(ParamType BookieId,long key,Object isWritable) {
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

        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = false;
        }

    }



    @Before
    public void set_up() throws Exception {


        if (bookieIdParamType.equals(ParamType.INVALID_CONFIG)) {

            this.bookieId = serverByIndex(0).getBookieId();

            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(),bcIsWritable,this.bookieId,1);
            pool.clients[0].setWritable(false);


        }

        if(bookieIdParamType.equals(ParamType.INVALID_INSTANCE)) {

            this.bookieId = serverByIndex(0).getBookieId();

            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(),bcIsWritable,this.bookieId,1);
            pool.clients[0].setWritable(false);

            bcIsWritable.channels.put(this.bookieId,pool);
        }

        if (bookieIdParamType.equals(ParamType.VALID_INSTANCE)){

            this.bookieId = serverByIndex(0).getBookieId();

            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(new ClientConfiguration(),bcIsWritable,this.bookieId,1);

            bcIsWritable.channels.put(this.bookieId,pool);

        }

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //BookieId,                     key,  ExpectedValue
                {ParamType.VALID_INSTANCE,     -1L,         true},
                {ParamType.INVALID_INSTANCE,   -1L,         false},
                {ParamType.INVALID_CONFIG,      2L,         new IllegalArgumentException()},
                {ParamType.NULL_INSTANCE,       1L,         new NullPointerException()}

        }) ;
    }

    @After
    public void tear_down() throws Exception {

        this.bcIsWritable.close();

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
                } catch (NullPointerException  | IllegalArgumentException e) {
                    Assert.assertEquals("Exception that i expect was raised", this.expectedIsWritable.getClass(), e.getClass());
                }

            } else Assert.assertEquals(this.expectedIsWritable, bcIsWritable.isWritable(this.bookieId, this.key));

        }
    }




}
