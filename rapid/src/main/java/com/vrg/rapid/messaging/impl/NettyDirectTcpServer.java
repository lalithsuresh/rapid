package com.vrg.rapid.messaging.impl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vrg.rapid.MembershipService;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;

/**
 * Uses Netty directly
 */
public class NettyDirectTcpServer implements IMessagingServer {
    private final int port;
    private final String hostname;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ExecutorService executorService;
    @Nullable private ChannelFuture channelFuture;
    @Nullable private MembershipService membershipService;

    public NettyDirectTcpServer(final Endpoint endpoint, final SharedResources resources) {
        this(endpoint.getHostname(), endpoint.getPort(), resources);
    }

    public NettyDirectTcpServer(final String hostname, final int port, final SharedResources resources) {
        this.hostname = hostname;
        this.port = port;
        this.bossGroup = resources.getEventLoopGroup();
        this.workerGroup = resources.getEventLoopGroup();
        this.executorService = resources.getServerExecutor();
    }

    @Override
    public void start() {
        try {
            final ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // (3)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_SNDBUF, 4096)
                    .option(ChannelOption.SO_RCVBUF, 4096)
                    .option(ChannelOption.SO_BACKLOG, 500)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(final SocketChannel ch) throws Exception {
                            ch.pipeline()
                              .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                              .addLast("frameEncoder", new LengthFieldPrepender(4))
                              .addLast(new RapidRequestHandler());
                        }
                    })
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            final ChannelFuture channelFuture = b.bind(hostname, port).sync(); // (7)
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void setMembershipService(final MembershipService service) {
        assert membershipService == null;
        membershipService = service;
    }


    /**
     * Handles a server-side channel.
     */
    private class RapidRequestHandler extends ChannelInboundHandlerAdapter { // (1)
        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            ctx.channel().close().addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) { // (2)
            final ByteBuf buf = (ByteBuf) msg;
            final long counter = buf.readLong();
            final int readableBytes = buf.readableBytes();
            try {
                final byte[] byteArray = new byte[readableBytes];
                buf.readBytes(byteArray);
                final RapidRequest request = RapidRequest.parseFrom(byteArray);
                if (membershipService != null) {
                    executorService.execute(()-> {
                        final ListenableFuture<RapidResponse> response = membershipService.handleMessage(request);
                        Futures.addCallback(response, new FutureCallback<RapidResponse>() {
                            @Override
                            @SuppressWarnings("all")
                            public void onSuccess(@Nullable final RapidResponse response) {
                                assert response != null;
                                final byte[] out = response.toByteArray();
                                final ByteBuf bufNew = ctx.alloc().buffer(out.length + (Long.SIZE / Byte.SIZE))
                                        .writeLong(counter)
                                        .writeBytes(response.toByteArray());
                                final ChannelFuture future = ctx.channel().writeAndFlush(bufNew, ctx.voidPromise());
                            }

                            @Override
                            public void onFailure(final Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        }, ctx.executor());
                    });
                }
            } catch (final InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) { // (4)
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close().addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }
    }
}