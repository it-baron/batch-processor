package com.akudsoft.butcher;

import com.akudsoft.lib.ShutdownHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

@ThreadSafe
public abstract class BatchProcessor<T extends BatchProcessor.HasBatchKey<K>, K> {
    public static final String DEFAULT_KEY = "DEFAULT";

    public interface HasBatchKey<K> {
        K getKey();
    }

    public interface HasNoKey extends HasBatchKey<String> {
        @Override
        default String getKey() {
            return DEFAULT_KEY;
        }
    }

    private static final int DEFAULT_BATCH_SIZE = 2000;
    private static final long DEFAULT_FLUSH_TIMEOUT = -1;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private final int concurrencyLevel;
    private long flushTimeout = DEFAULT_FLUSH_TIMEOUT;
    private final ConcurrentHashMap<K, BlockingQueue<T>> batches;
    private final ConcurrentHashMap<K, Long> timeouts;

    @SuppressWarnings("FieldCanBeLocal")
    private ScheduledExecutorService threadPool;

    protected BatchProcessor(int batchSize, int concurrencyLevel, long flushTimeout) {
        this.batchSize = batchSize;
        this.concurrencyLevel = concurrencyLevel;
        this.flushTimeout = flushTimeout;
        this.batches = new ConcurrentHashMap<>(this.batchSize, 0.9f, concurrencyLevel);
        this.timeouts = new ConcurrentHashMap<>(this.batchSize, 0.9f, concurrencyLevel);
        initializeScheduledFlush();

        ShutdownHandler.addAction(() -> {
            flushAll();

            if (threadPool != null) {
                threadPool.shutdown();
            }
        });
    }

    protected BatchProcessor(int batchSize, int concurrencyLevel) {
        this.batchSize = batchSize;
        this.concurrencyLevel = concurrencyLevel;
        this.batches = new ConcurrentHashMap<>(this.batchSize, 0.9f, concurrencyLevel);
        this.timeouts = new ConcurrentHashMap<>(this.batchSize, 0.9f, concurrencyLevel);
        initializeScheduledFlush();
    }

    private void initializeScheduledFlush() {
        if (this.threadPool != null) {
            this.threadPool.shutdown();
            this.threadPool = null;
        }

        if (flushTimeout > 0) {
            this.threadPool = Executors.newScheduledThreadPool(10);
            this.threadPool.scheduleWithFixedDelay((Runnable) this::flushAllExpired,
                    flushTimeout, flushTimeout, TimeUnit.MILLISECONDS);
        }
    }

    public void setFlushTimeout(long flushTimeout) {
        this.flushTimeout = flushTimeout;
        initializeScheduledFlush();
    }

    protected void put(T obj) {
        ensureQueue(obj.getKey());

        final List<T> arr = new ArrayList<>();
        batches.computeIfPresent(obj.getKey(), (k, q) -> {
            q.offer(obj);
            if (q.size() == batchSize) {
                q.drainTo(arr);
                return new LinkedBlockingQueue<>(batchSize);
            } else {
                return q;
            }
        });

        if (!arr.isEmpty()) {
            flush(obj.getKey(), arr);
        }
    }

    private void flushAllExpired() {
        timeouts.entrySet().stream()
                .filter(e -> System.currentTimeMillis() - e.getValue() >= flushTimeout)
                .forEach(e -> {
                    batches.computeIfPresent(e.getKey(), (k, q) -> {
                        List<T> arr = new ArrayList<>();
                        q.drainTo(arr);
                        flush(e.getKey(), arr);
                        return new LinkedBlockingQueue<>(batchSize);
                    });
                });
    }

    private BlockingQueue<T> ensureQueue(K key) {
        return batches.computeIfAbsent(key, $ -> {
            timeouts.computeIfAbsent(key, $$ -> System.currentTimeMillis());
            return new LinkedBlockingQueue<>(batchSize);
        });
    }

    protected boolean flush(K key, List<T> items) {
        if (items.size() == 0) return false;
        save(items);
        timeouts.computeIfPresent(key, (k, q) -> System.currentTimeMillis());
        return true;
    }

    protected abstract void save(List<T> itemsToSave);

    public void clear(K key) {
        ensureQueue(key).clear();
    }

    public int size(K key) {
        return ensureQueue(key).size();
    }

    private void flushAll() {
        this.batches.forEach(concurrencyLevel, (k, q) -> {
            List<T> arr = new ArrayList<>();
            q.drainTo(arr);
            flush(k, arr);
        });
    }

    public int totalSize() {
        return this.batches.reduceValuesToInt(concurrencyLevel, Collection::size, 0, (l, r) -> l + r);
    }

    public void clearAll() {
        this.batches.forEachKey(concurrencyLevel, this::clear);
    }
}