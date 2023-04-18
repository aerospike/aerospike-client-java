package com.aerospike.client.proxy.grpc;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.util.ArrayIterator;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * An event loop group containing a single event loop.
 * <p>
 * TODO: verify it is correct to delegate to the singleton event loop.
 */
class SingleEventLoopGroup implements EventLoopGroup {
    private final EventLoop eventLoop;

    SingleEventLoopGroup(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean isShuttingDown() {
        return eventLoop.isShuttingDown();
    }

    @Override
    public Future<?> shutdownGracefully() {
        return eventLoop.shutdownGracefully();
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        return eventLoop.shutdownGracefully(quietPeriod, timeout, unit);
    }

    @Override
    public Future<?> terminationFuture() {
        return eventLoop.terminationFuture();
    }

    @SuppressWarnings("deprecation")
	@Override
    public void shutdown() {
        eventLoop.shutdown();
    }

    @SuppressWarnings("deprecation")
	@Override
    public List<Runnable> shutdownNow() {
        return eventLoop.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return eventLoop.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return eventLoop.isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return eventLoop.awaitTermination(timeout, unit);
    }

    @Override
    public EventLoop next() {
        return eventLoop;
    }

    @Override
    public Iterator<EventExecutor> iterator() {
        return new ArrayIterator<>(new EventExecutor[]{eventLoop});
    }

    @Override
    public Future<?> submit(Runnable task) {
        return eventLoop.submit(task);
    }

    @NonNull
    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(@NonNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return eventLoop.invokeAll(tasks);
    }

    @NonNull
    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(@NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit) throws InterruptedException {
        return eventLoop.invokeAll(tasks, timeout, unit);
    }

    @NonNull
    @Override
    public <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return eventLoop.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks, long timeout, @NonNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return eventLoop.invokeAny(tasks, timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return eventLoop.submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return eventLoop.submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return eventLoop.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return eventLoop.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return eventLoop.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return eventLoop.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public ChannelFuture register(io.netty.channel.Channel channel) {
        return eventLoop.register(channel);
    }

    @Override
    public ChannelFuture register(ChannelPromise promise) {
        return eventLoop.register(promise);
    }

    @SuppressWarnings("deprecation")
	@Override
    public ChannelFuture register(io.netty.channel.Channel channel, ChannelPromise promise) {
        return eventLoop.register(channel, promise);
    }

    @Override
    public void execute(@NonNull Runnable command) {
        eventLoop.execute(command);
    }
}
