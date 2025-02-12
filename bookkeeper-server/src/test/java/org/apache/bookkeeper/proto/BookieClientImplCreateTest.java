package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.util.ParamType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


@RunWith(value = Parameterized.class)
public class BookieClientImplCreateTest{

    private Boolean exceptionInConfigPhase = false;
    private BookieClientImpl bookieClientImpl;


    //Test: create(BookieId address, PerChannelBookieClientPool pcbcPool,
    //            SecurityHandlerFactory shFactory, boolean forceUseV3)
    private Boolean expectedCreate = false;
    private PerChannelBookieClientPool pcbcPool;
    private ParamType pcbcPoolParamType;
    private SecurityHandlerFactory shFactory;
    private ClientConfiguration clientConfiguration;
    private BookieId bookieId;
    private boolean forceUseV3;


    public BookieClientImplCreateTest(ParamType bookieId, ParamType perChannelBookieClientPool, ParamType shFactory,boolean forceUseV3, boolean expectedCreate) {
        configureCreateTest(bookieId, perChannelBookieClientPool, shFactory, forceUseV3, expectedCreate);

    }

    private void configureCreateTest(ParamType bookieId, ParamType perChannelBookieClientPool, ParamType shFactory, boolean forceUseV3,boolean expectedCreate) {
        this.pcbcPoolParamType = perChannelBookieClientPool;
        this.forceUseV3 = forceUseV3;
        this.expectedCreate = expectedCreate;


        try {

            this.clientConfiguration = TestBKConfiguration.newClientConfiguration();
            OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().build();
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
            ByteBufAllocator byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler"));
            StatsLogger logger = NullStatsLogger.INSTANCE;
            BookieAddressResolver bookieAddressResolver = BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER;

            this.clientConfiguration.setLimitStatsLogging(true);


            this.bookieClientImpl = new BookieClientImpl(this.clientConfiguration,eventLoopGroup ,
                    byteBufAllocator, orderedExecutor,executorService , logger,
                    bookieAddressResolver);

            switch (shFactory) {
                case VALID_INSTANCE:
                    this.shFactory = null;
                    break;

            }

            switch (bookieId){
                case VALID_INSTANCE:
                    this.bookieId = BookieId.parse("Bookie-1");
                    break;

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedCreate = true;
                    break;

            }


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }





    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        //Test: create(BookieId address, PerChannelBookieClientPool pcbcPool,
                //            SecurityHandlerFactory shFactory, boolean forceUseV3)

        return Arrays.asList(new Object[][]{
                //Bookie_ID                      PerChannelBookieClientPool          SecurityHandlerFactory   forceUseV3, Raise Exception
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  false,    false},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  true,     false},
                { ParamType.VALID_INSTANCE,      ParamType.NULL_INSTANCE,            ParamType.VALID_INSTANCE,  false,    false},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE,  true,     true},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  false,    true},
                { ParamType.NULL_INSTANCE,       ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE,  false,    true},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.VALID_INSTANCE,  false,    true},


        }) ;
    }


    @Test
    public void test_CreateTest() {

        if (this.exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {
            if(this.pcbcPoolParamType == ParamType.INVALID_INSTANCE) {
                try {
                    this.pcbcPool = new DefaultPerChannelBookieClientPool(clientConfiguration, bookieClientImpl, this.bookieId, 0);
                    Assert.fail("Test case has failed");
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue("Invalid istance for PerChannelBookieClientPool", this.expectedCreate);
                }
            } else {

                try {
                    PerChannelBookieClient perChannelBookieClient = this.bookieClientImpl.create(this.bookieId, this.pcbcPool, this.shFactory, this.forceUseV3);
                    Assert.assertNotNull(perChannelBookieClient);
                    Assert.assertFalse("No exception was expected", this.expectedCreate);

                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue("An exception was expected",  this.expectedCreate);
                }
            }
        }
    }
}
