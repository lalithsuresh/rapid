package com.vrg.rapid.messaging.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.MembershipService;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple implementation of messaging over TCP with Netty. The main
 * advantage over gRPC is the decreased memory pressure (about 4x).
 */
public class NettyClientServer implements IMessagingClient, IMessagingServer {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientServer.class);
    private static final int RETRY_COUNT = 5;
    private static final FutureLoader FUTURE_LOADER = new FutureLoader();
    private final Endpoint listenAddress;
    private final LoadingCache<Endpoint, ChannelFuture> channelCache;
    private final LoadingCache<Long, SettableFuture<RapidResponse>> outstandingRequests;
    private final AtomicLong counter = new AtomicLong(0);
    private final SharedResources resources;

    @Nullable private MembershipService membershipService = null;
    @Nullable private ChannelFuture serverChannel = null;

    public NettyClientServer(final Endpoint listenAddress) {
        this(listenAddress, new SharedResources(listenAddress));
    }

    public NettyClientServer(final Endpoint listenAddress, final SharedResources resources) {
        this.listenAddress = listenAddress;
        this.outstandingRequests = CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .build(FUTURE_LOADER);
        this.resources = resources;

        // Bootstrap a client for sending messages. If this object is being used as a server instance,
        // Rapid will invoke the start() method which bootstraps a server.
        final Bootstrap clientBootstrap = new Bootstrap();
        final RemovalListener<Endpoint, ChannelFuture> removalListener = removal ->
                                 removal.getValue().channel().close()
                                         .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        this.channelCache = CacheBuilder.newBuilder()
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .removalListener(removalListener)
                .build(new ClientChannelLoader(clientBootstrap));
        final ClientHandler clientHandler = new ClientHandler();
        clientBootstrap.group(resources.getEventLoopGroup())
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_SNDBUF, 4096)
            .option(ChannelOption.SO_RCVBUF, 4096)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .handler(new ClientChannelInitializer(clientHandler));
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg) {
        // Configure the client.
        final Runnable onCallFailure = () -> channelCache.invalidate(remote);
        return Retries.callWithRetries(() -> {
            try {
                final long reqNo = counter.incrementAndGet();
                final SettableFuture<RapidResponse> future = outstandingRequests.get(reqNo);
                final ChannelFuture f = channelCache.get(remote);
                ignoreFuture(f.channel().writeAndFlush(new WrappedRapidRequest(reqNo, msg),
                                                       f.channel().voidPromise()));

                return future;
            } catch (final ExecutionException e) {
                return Futures.immediateFailedFuture(e);
            }
        }, remote, RETRY_COUNT, onCallFailure, resources.getBackgroundExecutor());
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessageBestEffort(final Endpoint remote, final RapidRequest msg) {
        return sendMessage(remote, msg);
    }

    /**
     * From IMessagingServer
     */
    @Override
    public void start() {
        // Bootstrap a server instance
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        final ServerHandler serverHandler = new ServerHandler();
        serverBootstrap.group(resources.getEventLoopGroup(), resources.getEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1000)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ServerChannelInitializer(serverHandler));
        try {
            serverChannel = serverBootstrap.bind(listenAddress.getHostname(), listenAddress.getPort()).sync();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Could not start server {}", e);
        }
    }


    /**
     * From IMessagingServer and IMessagingClient
     */
    @Override
    public void shutdown() {
        if (serverChannel != null) {
            serverChannel.channel().closeFuture().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
        channelCache.invalidateAll();
    }

    /**
     * From IMessagingServer
     */
    @Override
    public void setMembershipService(final MembershipService service) {
        if (service != null) {
            this.membershipService = service;
        } else {
            throw new IllegalArgumentException("null membership service instance supplied");
        }
    }


    private class ClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.debug("Client has successfully connected to the server {}", listenAddress);
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            LOG.debug("Closed connection to the server {}", listenAddress);
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            LOG.debug("Exception caught at client {}", cause);
            ctx.close().addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final WrappedRapidResponse wrappedRapidMessage = (WrappedRapidResponse) msg;
            receiveResponse(wrappedRapidMessage);
        }
    }

    @ChannelHandler.Sharable
    private class ServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.debug("Client has connected to the server {}", listenAddress);
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            LOG.debug("Client has disconnected from the server {}", listenAddress);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object obj) {
            final WrappedRapidRequest msg = (WrappedRapidRequest) obj;
            if (membershipService != null) {
                final RapidRequest request = msg.request;
                final ListenableFuture<RapidResponse> responseFuture = membershipService.handleMessage(request);
                Futures.addCallback(responseFuture, new FutureCallback<RapidResponse>() {
                    @Override
                    public void onSuccess(@Nullable final RapidResponse rapidResponse) {
                        if (rapidResponse != null) {
                            ignoreFuture(ctx.writeAndFlush(new WrappedRapidResponse(msg.count, rapidResponse),
                                                           ctx.voidPromise()));
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull final Throwable throwable) {
                        LOG.error("MessagingService returned an error {}", throwable);
                    }
                }, resources.getServerExecutor());
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            LOG.debug("Exception caught at server {}", cause);
            ctx.close().addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }
    }

    /**
     * Invoked at the client when the server responds to a message
     * @param message a RapidResponse + reqNo received after sending a RapidRequest with a given reqNo
     */
    private void receiveResponse(final WrappedRapidResponse message) {
        final RapidResponse rapidResponse = message.response;
        final SettableFuture<RapidResponse> future = outstandingRequests.getIfPresent(message.count);
        if (future != null) {
            future.set(rapidResponse);
            outstandingRequests.invalidate(message.count);
        } else {
            // Ignore
            LOG.error("Could not find future for req# {}", message.count);
        }
    }

    /**
     * Wraps Rapid messages to also carry a request number, used to route responses to the appropriate
     * ListenableFuture instances.
     */
    private static class WrappedRapidRequest implements Serializable {
        private static final long serialVersionUID = -4891729395L;
        private final long count;
        private final RapidRequest request;

        WrappedRapidRequest(final long count, final RapidRequest request) {
            this.count = count;
            this.request = request;
        }
    }

    private static class WrappedRapidResponse implements Serializable {
        private static final long serialVersionUID = -4891729395L;
        private final long count;
        private final RapidResponse response;

        WrappedRapidResponse(final long count, final RapidResponse response) {
            this.count = count;
            this.response = response;
        }
    }

    private static class FutureLoader extends CacheLoader<Long, SettableFuture<RapidResponse>> {
        @Override
        public SettableFuture<RapidResponse> load(@Nonnull final Long counter) {
            return SettableFuture.create();
        }
    }

    private static class ClientChannelLoader extends CacheLoader<Endpoint, ChannelFuture> {
        private final Bootstrap clientBootstrap;

        ClientChannelLoader(final Bootstrap clientBootstrap) {
            this.clientBootstrap = clientBootstrap;
        }

        @Override
        public ChannelFuture load(@Nonnull final Endpoint endpoint) throws Exception {
            // Connect to remote endpoint
            return clientBootstrap.connect(endpoint.getHostname(), endpoint.getPort()).sync();
        }
    }

    /**
     * Used in scenarios where we know we can ignore the returned Future of a method (like
     * when using void promises for example.
     *
     */
    @CanIgnoreReturnValue
    private static <T> ListenableFuture<T> ignoreFuture(final ChannelFuture future) {
        return Futures.immediateFuture(null);
    }

    private static class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ClientHandler clientHandler;

        ClientChannelInitializer(final ClientHandler clientHandler) {
            this.clientHandler = clientHandler;
        }

        @Override
        public void initChannel(final SocketChannel channel) {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new ObjectEncoder(),
                    new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(null)),
                    clientHandler);
        }
    }

    private static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ServerHandler serverHandler;

        ServerChannelInitializer(final ServerHandler serverHandler) {
            this.serverHandler = serverHandler;
        }

        @Override
        public void initChannel(final SocketChannel channel) {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new ObjectEncoder(),
                    new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(null)),
                    serverHandler);
        }
    }
}
