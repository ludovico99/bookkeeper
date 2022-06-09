package org.apache.bookkeeper.proto;

import com.google.common.collect.Lists;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.client.ServerTester;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;


@RunWith(value = Parameterized.class)
public class BookieClientImplTest{

    private BookieClientImpl bc;

    //Test: GetFaultyBookies()
    private List<ServerTester> bookies;
    private int nFaultyBookies;
    private List<BookieId> expectedFaultyBookies;

    private ClientConfiguration conf;


    public BookieClientImplTest(int nFaultyBookies, List<BookieId> expectedFaultyBookiesList){
        configure(nFaultyBookies,expectedFaultyBookiesList);
    }

    public void configure(int nFaultyBookies, List<BookieId> expectedFaultyBookiesList) {

        try {
            this.conf = TestBKConfiguration.newClientConfiguration();
            this.nFaultyBookies = nFaultyBookies;
            this.bookies = new ArrayList<>();
            this.expectedFaultyBookies = expectedFaultyBookiesList;

            if (nFaultyBookies > 0) {
                for (int i = 0; i<nFaultyBookies;i++) {
                    ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
                    ServerTester server = startBookie(serverConfiguration);
                    this.bookies.add(server);
                    this.expectedFaultyBookies.add(server.getServer().getBookieId());
                }

            }

            this.bc = new BookieClientImpl(conf, new NioEventLoopGroup(),
                    UnpooledByteBufAllocator.DEFAULT, OrderedExecutor.newBuilder().build(), Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientScheduler")), NullStatsLogger.INSTANCE,
                    BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                //N.di Faulty bookies, expected list di Faulty bookies
                {0,   Lists.newArrayList()},
                {2, Lists.newArrayList()},
                {10, Lists.newArrayList()}
        });
    }

    @After
    public void tear_down() throws Exception {
        for (ServerTester server : this.bookies) server.shutdown();
    }


    @Test
    public void Test_GetFaultyBookies() throws UnknownHostException {

        long threshold = conf.getBookieErrorThresholdPerInterval();

        for (int i = 0; i <nFaultyBookies;i++) {

            PerChannelBookieClientPool pool = bc.lookupClient(bookies.get(i).getServer().getBookieId());

            ((DefaultPerChannelBookieClientPool) pool).errorCounter.getAndSet((int) ++threshold);

            bc.channels.put(bookies.get(i).getServer().getBookieId(), pool);

        }

        this.expectedFaultyBookies.sort(Comparator.comparing(BookieId::getId));

        List<BookieId> actualFaultyBookies = this.bc.getFaultyBookies();

        actualFaultyBookies.sort(Comparator.comparing(BookieId::getId));

        Assert.assertEquals(this.expectedFaultyBookies, actualFaultyBookies);
    }

//    @Test(expected = NullPointerException.class)
//    public void Test_GetNumPendingRequests(){
//       bc.getNumPendingRequests(null,1);
//    }

    private ServerTester startBookie(ServerConfiguration conf) throws Exception {

        ServerTester tester = new ServerTester(conf);
        tester.getServer().start();
        return tester;

    }

}
