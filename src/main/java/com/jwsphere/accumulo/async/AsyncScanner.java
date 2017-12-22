package com.jwsphere.accumulo.async;

import java.util.concurrent.Executor;

public interface AsyncScanner {

    void scan(ScanObserver observer);

    void scanOn(ScanObserver observer, Executor executor);

}
