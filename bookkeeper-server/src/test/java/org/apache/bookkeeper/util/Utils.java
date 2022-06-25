package org.apache.bookkeeper.util;

public class Utils {
    public static void sleep(long millis) {
        try {
            if (millis >0 ) Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
