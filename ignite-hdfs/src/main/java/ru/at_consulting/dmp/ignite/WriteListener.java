package ru.at_consulting.dmp.ignite;

import java.util.concurrent.CountDownLatch;

/**
 * Created by DAIvanov on 26.08.2016.
 */
public class WriteListener {
    private final CountDownLatch latch;

    public WriteListener(int nWrites) {
        this.latch = new CountDownLatch(nWrites);
    }
    public void writeEnd() {
        latch.countDown();
    }
    public void listen() throws InterruptedException {
        latch.await();
    }
}
