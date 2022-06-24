package org.apache.bookkeeper.proto;

import com.google.common.collect.Lists;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;


@RunWith(value = Parameterized.class)
public class BookieClientImplGetFaultyBookiesTest extends BookKeeperClusterTestCase {

    private  Boolean exceptionInConfigPhase = false;
    private  BookieClientImpl bookieClientImpl;
    private  ClientConfiguration confFaultyBookies;

    //Test: GetFaultyBookies()
    private int nFaultyBookies;
    private List<BookieId> expectedFaultyBookies;


    public BookieClientImplGetFaultyBookiesTest(int nFaultyBookie) {
        super(0);
        configureGetFaultyBookies(nFaultyBookie);

    }


    @Before
    public void set_up() throws Exception {

        ServerConfiguration serverConfiguration = newServerConfiguration();
        serverConfiguration.setMetadataServiceUri(zkUtil.getMetadataServiceUri("/ledgers"));
        startAndAddBookie(serverConfiguration);

        if (nFaultyBookies > 0) {
            for (int i = 1; i<= nFaultyBookies;i++) {
                serverConfiguration = newServerConfiguration();
                serverConfiguration.setMetadataServiceUri(zkUtil.getMetadataServiceUri("/ledgers"));
                ServerTester server = startAndAddBookie(serverConfiguration);
                this.expectedFaultyBookies.add(server.getServer().getBookieId());
            }
        }


    }




    private void configureGetFaultyBookies(int nFaultyBookies) {

        try {
            this.nFaultyBookies = nFaultyBookies;
            this.expectedFaultyBookies = Lists.newArrayList();
            this.confFaultyBookies = TestBKConfiguration.newClientConfiguration();

            this.bookieClientImpl = new BookieClientImpl(this.confFaultyBookies, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }

    }



    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //N.di Faulty bookies
                {10},
                {0}
        });
    }


    @Test
    public void test_GetFaultyBookies() throws Exception {

        if (exceptionInConfigPhase)
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        else {

            long threshold = confFaultyBookies.getBookieErrorThresholdPerInterval();

            DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(confFaultyBookies, bookieClientImpl,
                    serverByIndex(0).getBookieId(), 1);

            pool.errorCounter.getAndSet((int) --threshold);

            bookieClientImpl.channels.put(serverByIndex(0).getBookieId(), pool);

            for (int i = 1; i <= nFaultyBookies; i++) {

                pool = new DefaultPerChannelBookieClientPool(confFaultyBookies, bookieClientImpl,
                        serverByIndex(i).getBookieId(), 1);

                pool.errorCounter.getAndSet((int) ++threshold);

                bookieClientImpl.channels.put(serverByIndex(i).getBookieId(), pool);

            }


            this.expectedFaultyBookies.sort(Comparator.comparing(BookieId::getId));

            List<BookieId> actualFaultyBookies = bookieClientImpl.getFaultyBookies();

            actualFaultyBookies.sort(Comparator.comparing(BookieId::getId));

            Assert.assertEquals(this.expectedFaultyBookies, actualFaultyBookies);
        }
    }


}
