package com.vrg.rapid.messaging.impl;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Uses netty directly for TCP-based messaging.
 */
public class NettyDirectTcpClient implements IMessagingClient {
    private final AtomicLong messageCounter;
    private final Bootstrap bootstrap;
    private final ConcurrentHashMap<Endpoint, ChannelFuture> channels;
    private final ConcurrentHashMap<Long, SettableFuture<RapidResponse>> outstandingRequests;
    private final ScheduledExecutorService backgroundExecutor;

    public NettyDirectTcpClient(final SharedResources resources) {
        this.messageCounter = new AtomicLong(0);
        this.outstandingRequests = new ConcurrentHashMap<>();
        this.backgroundExecutor = resources.getBackgroundExecutor();
        this.channels = new ConcurrentHashMap<>();
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(resources.getEventLoopGroup())
            .channel(NioSocketChannel.class)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.SO_SNDBUF, 4096)
            .option(ChannelOption.SO_RCVBUF, 4096)
            .option(ChannelOption.SO_BACKLOG, 500)
            .option(ChannelOption.SO_REUSEADDR, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) throws Exception {
                    ch.pipeline()
                      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                      .addLast("frameEncoder", new LengthFieldPrepender(4))
                      .addLast(new RapidResponseHandler());
                }
            });
    }

    /**
     * Sends a message with retries
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg) {
        final int retries = GrpcClient.DEFAULT_GRPC_DEFAULT_RETRIES;
        final Supplier<ListenableFuture<RapidResponse>> call = () -> sendMessageBestEffort(remote, msg);
        return Retrier.callWithRetries(call, remote, retries, backgroundExecutor);
    }

    /**
     * Sends a message without retries. Using UDP here is an option but it would require falling back
     * to TCP for larger message sizes anyway.
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessageBestEffort(final Endpoint remote, final RapidRequest msg) {
        return Futures.withTimeout(sendOnce(remote, msg.toByteArray()), getTimeoutForMessageMs(msg),
                TimeUnit.MILLISECONDS, backgroundExecutor);
    }

    /**
     * One-to-all TCP based broadcast
     */
    @Override
    public List<ListenableFuture<RapidResponse>> bestEffortBroadcast(final List<Endpoint> endpointList,
                                                                     final RapidRequest msg) {
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>(endpointList.size());
        final byte[] bytesToSend = msg.toByteArray();
        for (final Endpoint ep: endpointList) {
            responses.add(sendOnce(ep, bytesToSend));
        }
        return responses;
    }

    @Override
    public void shutdown() {
        channels.values().forEach(c -> c.channel().close()
                .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE));
    }

    /**
     * Sends a byte array and registers a SettableFuture to be invoked when a response arrive.
     */
    private ListenableFuture<RapidResponse> sendOnce(final Endpoint remote, final byte[] bytesToSend) {
        final SettableFuture<RapidResponse> responseSettableFuture = SettableFuture.create();
        final ChannelFuture future = channels.computeIfAbsent(remote, this::connect);
        future.addListener((ChannelFutureListener) channelFuture ->
                send(channelFuture, remote, bytesToSend, responseSettableFuture));
        return responseSettableFuture;
    }

    /**
     * Sends a byte array on the channel and closes the connection if we experience failures. This forces
     * sendOnce() to re-establish connections if required.
     */
    private void send(final ChannelFuture channelFuture, final Endpoint remote, final byte[] bytesToSend,
                      final SettableFuture<RapidResponse> responseSettableFuture) {
        if (channelFuture.isSuccess()) {
            final Channel channel = channelFuture.channel();
            final long counter = messageCounter.incrementAndGet();
            final ByteBuf buf = channel.alloc().buffer((Long.SIZE / Byte.SIZE) + bytesToSend.length)
                    .writeLong(counter).writeBytes(bytesToSend);
            outstandingRequests.put(counter, responseSettableFuture);
            channel.writeAndFlush(buf)
                   .addListener((ChannelFutureListener) channelFuture1 -> {
                       if (!channelFuture1.isSuccess()) {
                           channels.remove(remote);
                           responseSettableFuture.setException(channelFuture1.cause());
                       }
                   });
        }
        else {
            channels.remove(remote);
            responseSettableFuture.setException(channelFuture.cause());
        }
    }

    /**
     * Establish a channel to a remote peer.
     */
    private ChannelFuture connect(final Endpoint remote) {
        return bootstrap.connect(remote.getHostname(), remote.getPort());
    }

    /**
     * Receives responses from NettyDirectTcpServer. The message ID is used to invoke the appropriate SettableFuture.
     */
    public class RapidResponseHandler extends SimpleChannelInboundHandler<Object> {
        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext,
                                    final Object o) throws Exception {
            final ByteBuf buf = (ByteBuf) o;
            final long counter = buf.readLong();
            final byte[] byteArray = new byte[buf.readableBytes()];
            buf.readBytes(byteArray);
            final RapidResponse response = RapidResponse.parseFrom(byteArray);
            outstandingRequests.remove(counter).set(response);
        }
    }

    /**
     * TODO: These timeouts should be on the Rapid side of the IMessagingClient API.
     *
     * @param msg RapidRequest
     * @return timeout to use for the RapidRequest message
     */
    private int getTimeoutForMessageMs(final RapidRequest msg) {
        switch (msg.getContentCase()) {
            case PROBEMESSAGE:
                return GrpcClient.DEFAULT_GRPC_PROBE_TIMEOUT;
            case JOINMESSAGE:
                return GrpcClient.DEFAULT_GRPC_JOIN_TIMEOUT;
            default:
                return GrpcClient.DEFAULT_GRPC_TIMEOUT_MS;
        }
    }
}