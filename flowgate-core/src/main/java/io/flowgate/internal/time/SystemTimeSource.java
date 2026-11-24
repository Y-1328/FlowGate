
package io.flowgate.internal.time;

public final class SystemTimeSource implements TimeSource {

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
