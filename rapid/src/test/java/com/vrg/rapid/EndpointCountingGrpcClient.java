package com.vrg.rapid;

import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class EndpointCountingGrpcClient extends GrpcClient {

    private final Set<Endpoint> seenEndpoints = ConcurrentHashMap.newKeySet();

    EndpointCountingGrpcClient(final Endpoint address, final ISettings settings) {
        super(address, settings);
    }

    @Override
    public ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg) {
        seenEndpoints.add(remote);
        return super.sendMessage(remote, msg);
    }

    @Override
    public ListenableFuture<RapidResponse> sendMessageBestEffort(final Endpoint remote, final RapidRequest msg) {
        seenEndpoints.add(remote);
        return super.sendMessageBestEffort(remote, msg);
    }

    public int getEndpointCount() {
        return seenEndpoints.size();
    }
}
