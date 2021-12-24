package org.apache.rocketmq.store.anyDelay;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AnyDelayDeliveryService {

    private HashedWheelTimer timer;
    private AtomicBoolean started = new AtomicBoolean();

    public AnyDelayDeliveryService() {
        this.timer = new HashedWheelTimer();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            this.timer.stop();
        }
    }
    public void addTimeTask(TimerTask timerTask, long delaySecond){
        this.timer.newTimeout(timerTask,delaySecond, TimeUnit.SECONDS);
    }

    public long getTaskCount(){
        return timer.pendingTimeouts();
    }
}
