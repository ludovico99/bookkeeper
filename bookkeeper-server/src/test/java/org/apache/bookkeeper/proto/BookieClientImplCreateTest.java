package org.apache.bookkeeper.proto;

import com.google.protobuf.ExtensionRegistry;
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
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.util.ClientConfType;
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
    private Object expectedCreate = false;
    private PerChannelBookieClientPool pcbcPool;
    private ParamType pcbcPoolParamType;
    private SecurityHandlerFactory shFactory;
    private ClientConfiguration clientConfiguration;
    private BookieId bookieId;
    private boolean forceUseV3;


    public BookieClientImplCreateTest(ParamType bookieId, ParamType perChannelBookieClientPool, ParamType shFactory,boolean forceUseV3) {
        configureForceLedger(bookieId, perChannelBookieClientPool, shFactory, forceUseV3);

    }

    private void configureForceLedger(ParamType bookieId, ParamType perChannelBookieClientPool, ParamType shFactory, boolean forceUseV3) {
        this.pcbcPoolParamType = perChannelBookieClientPool;
        this.forceUseV3 = forceUseV3;


        try {

            clientConfiguration = TestBKConfiguration.newClientConfiguration();
            OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().build();
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
            ByteBufAllocator byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler"));
            StatsLogger logger = NullStatsLogger.INSTANCE;
            BookieAddressResolver bookieAddressResolver = BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER;
            ExtensionRegistry registry = ExtensionRegistry.newInstance();

            clientConfiguration.setLimitStatsLogging(true);

            ClientAuthProvider.Factory factory = AuthProviderFactoryFactory.newClientAuthProviderFactory(clientConfiguration);

            this.bookieClientImpl = new BookieClientImpl(clientConfiguration,eventLoopGroup ,
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
                    switch (perChannelBookieClientPool) {
                        case VALID_INSTANCE:
                            this.pcbcPool = new DefaultPerChannelBookieClientPool(clientConfiguration,bookieClientImpl,
                                    this.bookieId,1);
                            this.expectedCreate = new PerChannelBookieClient(clientConfiguration,orderedExecutor,eventLoopGroup,
                                    byteBufAllocator,this.bookieId,logger,factory,registry,this.pcbcPool,this.shFactory,bookieAddressResolver);
                            break;

                        case NULL_INSTANCE:
                            this.pcbcPool = null;
                            break;

                        case INVALID_INSTANCE:
                            this.expectedCreate = true;
                            break;


                    }

                case NULL_INSTANCE:
                    this.bookieId = null;
                    this.expectedCreate = true;
                    break;

            }


        }catch (Exception e){
            e.printStackTrace();
            //exceptionInConfigPhase = true;
        }

    }





    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        //Test: create(BookieId address, PerChannelBookieClientPool pcbcPool,
                //            SecurityHandlerFactory shFactory, boolean forceUseV3)

        return Arrays.asList(new Object[][]{
                //Bookie_ID                      PerChannelBookieClientPool          SecurityHandlerFactory   forceUseV3
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  false},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  true},
                { ParamType.NULL_INSTANCE,       ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  false},
                { ParamType.NULL_INSTANCE,       ParamType.NULL_INSTANCE,            ParamType.VALID_INSTANCE,  false},
                { ParamType.VALID_INSTANCE,      ParamType.VALID_INSTANCE,           ParamType.VALID_INSTANCE,  true},
                { ParamType.VALID_INSTANCE,      ParamType.INVALID_INSTANCE,         ParamType.VALID_INSTANCE,  true}

        }) ;
    }


    @Test
    public void test_forceLedger() {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                    " been thrown.", true);
        else {
            try {

                if(this.pcbcPoolParamType.equals(ParamType.INVALID_INSTANCE)) this.pcbcPool = new DefaultPerChannelBookieClientPool(clientConfiguration,bookieClientImpl,
                        this.bookieId,0);

                Assert.assertEquals(this.expectedCreate, bookieClientImpl.create(this.bookieId, this.pcbcPool, this.shFactory, this.forceUseV3));
                Assert.fail("Test case has failed");

            } catch (Exception e){
                Assert.assertTrue("An exception was expected", (Boolean) this.expectedCreate);
            }

        }
    }

}
