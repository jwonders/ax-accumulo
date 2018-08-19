package com.jwsphere.accumulo.async;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

public class BlockingScanner {

    private final AsyncScanner scanner;

    public BlockingScanner(AsyncScanner scanner) {
        this.scanner = Objects.requireNonNull(scanner);
    }

    public Iterator<Cell> iterator() {
        BlockingSubscriber observer = new BlockingSubscriber();
        scanner.subscribe(observer);
        return observer;
    }

    /**
     * A scan observer that requests a single message at a time and waits
     * until the consumer has
     */
    private static class BlockingSubscriber implements ScanSubscriber, Iterator<Cell> {

        private static final Object DONE = new Object();

        private ScanSubscription controller;

        private final SynchronousQueue<Object> queue;
        private Object next;

        BlockingSubscriber() {
            this.queue = new SynchronousQueue<>();
        }

        @Override
        public void onSubscribe(ScanSubscription subscription) {
            this.controller = subscription;
            this.controller.request(1);
        }

        @Override
        public void onNext(Cell cell) {
            if (controller == null) {
                throw new IllegalStateException("Observer must be initialized");
            }
            try {
                this.queue.put(cell);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onComplete() {
            try {
                this.queue.put(DONE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            try {
                next = this.queue.take();
                return next != null && next != DONE;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        public Cell next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Cell cell =  (Cell) next;
            next = null;
            return cell;
        }

    }

}
