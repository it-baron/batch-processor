package com.akudsoft.butcher;

import com.akudsoft.butcher.BatchProcessor.HasBatchKey;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class BatchProcessorTest {
    private static final int BATCH_SIZE = 1000;
    private static final long FLUSH_TIMEOUT = 10000;
    private static final String KEY = "key";
    public static final int SAVE_TIME_MILLIS = 1;

    private static class TestItem implements HasBatchKey<String> {
        private String id = KEY;

        public String getId() {
            return id;
        }

        public TestItem() {
        }

        public TestItem(String id) {
            this.id = id;
        }

        @Override
        public String getKey() {
            return id;
        }
    }

    @Test
    public void shouldPutItem() {
        final BatchProcessor<TestItem, String> processor = new TestBatchProcessor(BATCH_SIZE, 1, FLUSH_TIMEOUT);
        processor.setFlushTimeout(-1);

        processor.put(new TestItem());
        processor.put(new TestItem());
        processor.put(new TestItem());

        assertEquals(3, processor.size(KEY));

        processor.clear(KEY);
        assertEquals(processor.size(KEY), 0);
    }

    @Test
    public void shouldFlushItemsOnBatchSize() {
        final TestBatchProcessor processor = new TestBatchProcessor(4, 1);
        processor.setFlushTimeout(-1);

        processor.put(new TestItem());
        processor.put(new TestItem());
        processor.put(new TestItem());
        processor.put(new TestItem());
        assertEquals(0, processor.size(KEY));

        processor.put(new TestItem());
        assertEquals(1, processor.size(KEY));
        assertEquals(1, processor.flushCalled.get());
        assertEquals(1, processor.saveCalled.get());
    }

    @Test
    public void shouldPutByKeyQueue() {
        final BatchProcessor<TestItem, String> processor = new TestBatchProcessor(4, 1);

        final String aid1 = "aid1";
        final String aid2 = "aid2";

        processor.put(new TestItem(aid1));
        processor.put(new TestItem(aid1));
        processor.put(new TestItem(aid1));

        processor.put(new TestItem(aid2));
        processor.put(new TestItem(aid2));

        assertEquals(3, processor.size(aid1));
        assertEquals(2, processor.size(aid2));
        assertEquals(5, processor.totalSize());

        processor.put(new TestItem(aid1));
        assertEquals(0, processor.size(aid1));
        assertEquals(2, processor.size(aid2));
        assertEquals(2, processor.totalSize());
    }

    // check flush timeout
    @Test
    public void shouldBeFlushedAfterTimeout() throws InterruptedException {
        final TestBatchProcessor processor = new TestBatchProcessor(40, 1);
        final int flushTimeout = 200;

        processor.setFlushTimeout(flushTimeout);

        processor.put(new TestItem());
        processor.put(new TestItem());
        processor.put(new TestItem());
        assertEquals(3, processor.size(KEY));

        TimeUnit.MILLISECONDS.sleep(flushTimeout * 3 + SAVE_TIME_MILLIS);

        assertEquals(0, processor.size(KEY));
        assertEquals(2, processor.flushCalled.get());
        assertEquals(1, processor.saveCalled.get());
    }

    @Test
    public void testInLoadParallel() throws InterruptedException {
        final int threadCount = 20;
        final int itemsCount = 50000;
        final int batchSize = 500;

        final TestBatchProcessor processor = new TestBatchProcessor(batchSize, threadCount);

        final String aid1 = "aid1";
        final String aid2 = "aid2";
        final String aid3 = "aid3";

        final ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        AtomicInteger aid1cnt = new AtomicInteger(0);
        AtomicInteger aid2cnt = new AtomicInteger(0);
        AtomicInteger aid3cnt = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            threadPool.submit((Runnable) () -> {
                for (int j = 0; j < itemsCount / 2; j++) {
                    processor.put(new TestItem(aid1));
                    aid1cnt.incrementAndGet();
                }

                for (int j = 0; j < itemsCount / 4; j++) {
                    processor.put(new TestItem(aid2));
                    aid2cnt.incrementAndGet();
                }

                for (int j = 0; j < itemsCount / 4; j++) {
                    processor.put(new TestItem(aid3));
                    aid3cnt.incrementAndGet();
                }
            });
        }

        threadPool.shutdown();
        threadPool.awaitTermination((itemsCount / batchSize) * 20 * 200 * 2, TimeUnit.MILLISECONDS);

        assertEquals(0, processor.size(aid1));
        assertEquals(0, processor.size(aid2));
        assertEquals(0, processor.size(aid3));

        processor.put(new TestItem(aid1));
        processor.put(new TestItem(aid2));
        processor.put(new TestItem(aid3));

        assertEquals(1, processor.size(aid1));
        assertEquals(1, processor.size(aid2));
        assertEquals(1, processor.size(aid3));

        assertEquals(2000, processor.flushCalled.get());
        assertEquals(2000, processor.saveCalled.get());
    }

    public static class TestBatchProcessor extends BatchProcessor<TestItem, String> {
        private AtomicInteger flushCalled = new AtomicInteger(0);
        private AtomicInteger saveCalled = new AtomicInteger(0);

        public TestBatchProcessor(int batchSize, int concurrencyLevel, long flushTimeout) {
            super(batchSize, concurrencyLevel, flushTimeout);
        }

        public TestBatchProcessor(int batchSize, int concurrencyLevel) {
            super(batchSize, concurrencyLevel);
        }

        @Override
        protected void save(List<TestItem> itemsToSave) {
            try {
                Thread.sleep(SAVE_TIME_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            saveCalled.incrementAndGet();
        }

        @Override
        public boolean flush(String key, List<TestItem> items) {
            super.flush(key, items);
            flushCalled.incrementAndGet();
            return true;
        }
    }
}