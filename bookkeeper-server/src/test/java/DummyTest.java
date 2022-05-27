import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class DummyTest {

    @Test(expected = Exception.class)
    public void createLedgerTest() throws BKException, IOException, InterruptedException {
        BookKeeper bookKeeper = new BookKeeper("servers");
        long now = System.currentTimeMillis() / 1000;
        LedgerHandle handle = bookKeeper.createLedger(3,3, BookKeeper.DigestType.CRC32, "password".getBytes());

        long ledgerCreationTime = handle.getCtime();
        boolean isAfter = ledgerCreationTime > now;
        assertTrue(isAfter);
    }
} 