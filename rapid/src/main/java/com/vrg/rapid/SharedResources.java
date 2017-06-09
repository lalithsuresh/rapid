package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lsuresh on 5/30/17.
 */
class SharedResources {
    private final EventLoopGroup eventLoopGroup;
    private final ExecutorService backgroundExecutor;
    private final ExecutorService serverExecutor;
    private final ExecutorService clientChannelExecutor;
    private final ExecutorService protocolExecutor;

    SharedResources(final HostAndPort address) {
        this.eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("elg", true));
        this.backgroundExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("bg-" + address + "-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler(
                        (t, e) -> System.err.println(String.format("bg-executor caught exception: %s %s", t, e))
                ).build());
        this.serverExecutor = Executors.newFixedThreadPool(1,
                                new DefaultThreadFactory("server-exec", true));
        this.clientChannelExecutor = Executors.newFixedThreadPool(1,
                                new DefaultThreadFactory("client-exec", true));
        this.protocolExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("protocol-" + address + "-%d")
                .setUncaughtExceptionHandler(
                    (t, e) -> System.err.println(String.format("Server protocolExecutor caught exception: %s %s", t, t))
                ).build());
    }

    EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    ExecutorService getBackgroundExecutor() {
        return backgroundExecutor;
    }

    ExecutorService getServerExecutor() {
        return serverExecutor;
    }

    ExecutorService getClientChannelExecutor() {
        return clientChannelExecutor;
    }

    ExecutorService getProtocolExecutor() {
        return protocolExecutor;
    }

    void shutdown() {
        serverExecutor.shutdownNow();
        protocolExecutor.shutdownNow();
        eventLoopGroup.shutdownGracefully();
    }
}
