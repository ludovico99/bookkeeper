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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Executors;



@RunWith(value = Parameterized.class)
public class BookieClientImplTestB extends BookKeeperClusterTestCase {

    private Boolean exceptionInConfigPhase = false;

    //Test: GetFaultyBookies()
    private ClientConfiguration confFaultyBookies;
    private BookieClientImpl bcFaultyBookies;
    private int nFaultyBookies;
    private List<BookieId> expectedFaultyBookies;


    public BookieClientImplTestB(int nFaultyBookies, List<BookieId> faultyBookies) {
        super(0);
        configureGetFaultyBookies(nFaultyBookies,  faultyBookies);

    }


    @Before
    public void set_up() throws Exception {

        if (nFaultyBookies > 0) {
            for (int i = 0; i<nFaultyBookies;i++) {
                ServerConfiguration serverConfiguration = newServerConfiguration();
                serverConfiguration.setMetadataServiceUri(zkUtil.getMetadataServiceUri("/ledgers"));
                ServerTester server = startAndAddBookie(serverConfiguration);
                this.expectedFaultyBookies.add(server.getServer().getBookieId());
            }
        }


    }




    private void configureGetFaultyBookies(int nFaultyBookies, List<BookieId> expectedFaultyBookiesList) {

        try {

            this.confFaultyBookies = TestBKConfiguration.newClientConfiguration();

            this.bcFaultyBookies = new BookieClientImpl(confFaultyBookies, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);


            this.nFaultyBookies = nFaultyBookies;
            this.expectedFaultyBookies = expectedFaultyBookiesList;


        }catch (Exception e){
            e.printStackTrace();
            this.exceptionInConfigPhase = false;
        }

    }



    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //N.di Faulty bookies,   expected list di Faulty bookies
                {10, Lists.newArrayList()},
                {-1, Lists.newArrayList()},
                {0, Lists.newArrayList()}
        });
    }

    @After
    public void tear_down() throws Exception {

        this.bcFaultyBookies.close();

        for (int i=0 ; i< numBookies;i++) {
            serverByIndex(i).getBookie().shutdown();
            serverByIndex(i).shutdown();

        }
    }


    @Test
    public void test_GetFaultyBookies() throws Exception {

        if (this.exceptionInConfigPhase)   Assert.assertTrue("No exception was expected, but an exception during configuration phase has" +
                " been thrown.", true);
        else {

            long threshold = confFaultyBookies.getBookieErrorThresholdPerInterval();

            for (int i = 0; i < nFaultyBookies; i++) {

                DefaultPerChannelBookieClientPool pool = new DefaultPerChannelBookieClientPool(this.confFaultyBookies, bcFaultyBookies,
                        serverByIndex(i).getBookieId(), 10);

                pool.errorCounter.getAndSet((int) ++threshold);

                bcFaultyBookies.channels.put(serverByIndex(i).getBookieId(), pool);

            }

            this.expectedFaultyBookies.sort(Comparator.comparing(BookieId::getId));

            List<BookieId> actualFaultyBookies = this.bcFaultyBookies.getFaultyBookies();

            actualFaultyBookies.sort(Comparator.comparing(BookieId::getId));

            Assert.assertEquals(this.expectedFaultyBookies, actualFaultyBookies);
        }
    }


}
