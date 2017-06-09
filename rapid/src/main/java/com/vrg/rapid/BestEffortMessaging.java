package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.BroadcastMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.internal.SocketUtils;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by lsuresh on 6/9/17.
 */
class BestEffortMessaging {
    private final HostAndPort listenAddress;
    private final MembershipService membershipService;
    private final SharedResources resources;

    @Nullable private Channel channelServer;
    @Nullable private ChannelPromise promise;

    BestEffortMessaging(final HostAndPort listenAddress,
              final MembershipService service,
              final SharedResources resources) {
        this.listenAddress = listenAddress;
        this.membershipService = service;
        this.resources = resources;
    }

    void start() {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(resources.getEventLoopGroup())
             .channel(NioDatagramChannel.class)
             .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
             .handler(new ChannelInitializer<NioDatagramChannel>() {
                @Override
                public void initChannel(final NioDatagramChannel ch) throws Exception {
                    final ChannelPipeline p = ch.pipeline();
                    p.addLast(new MessageHandler());
                }
             });
        // Bind and start to accept incoming connections.
        try {
            channelServer = bootstrap.bind(listenAddress.getHost(), listenAddress.getPort()).sync().channel();
            promise = channelServer.voidPromise();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void send(final BroadcastMessage msg, final List<HostAndPort> remoteNodes) {
        assert channelServer != null;
        resources.getBackgroundExecutor().execute(() -> {
            ByteBuf byteBuf = null;
            try {
                final byte[] bytes = msg.toByteArray();
                byteBuf = channelServer.alloc().buffer(bytes.length);
                byteBuf.writeBytes(bytes);
                for (final HostAndPort remote : remoteNodes) {
                    if (remote.equals(listenAddress)) {
                        processByType(msg);
                        continue;
                    }
                    final DatagramPacket packet = new DatagramPacket(byteBuf.retain(),
                            SocketUtils.socketAddress(remote.getHost(), remote.getPort()));
                    channelServer.writeAndFlush(packet, promise);
                }
            } finally {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        });
    }

    void shutdown() {
    }

    private class MessageHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext,
                                    final DatagramPacket datagramPacket) throws Exception {
            final ByteBuf byteBuf = datagramPacket.content();
            final ByteBufInputStream stream = new ByteBufInputStream(byteBuf);
            processByType(BroadcastMessage.parseFrom(stream));
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            cause.printStackTrace();
            // We don't close the channel because we can keep serving requests.
        }
    }

    private void processByType(final BroadcastMessage msg) {
        switch (msg.getSubMsgCase()) {
            case LINKUPDATEMESSAGE:
                resources.getProtocolExecutor().execute(
                        () -> membershipService.processLinkUpdateMessage(msg.getLinkUpdateMessage()));
                break;
            case CONSENSUSPROPOSAL:
                resources.getProtocolExecutor().execute(
                        () -> membershipService.processConsensusProposal(msg.getConsensusProposal()));
                break;
            case SUBMSG_NOT_SET:
                throw new RuntimeException("Payload not set");
            default:
                throw new RuntimeException("Invalid message");
        }
    }

}

