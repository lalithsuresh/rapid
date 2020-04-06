package com.vrg.rapid;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExperimentalApi;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.BroadcastingMessage;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.Proposal;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Broadcaster based on a ring of broadcasting nodes organized via consistent hashing.
 *
 * Nodes in the cluster are segregated in two groups:
 * - broadcasting nodes (B) that will relay messages to be broadcasted to all
 * - "broadastee" nodes (n) that send the messages to be broadcasted to "their" broadcaster
 *
 * The goal is to minimize the amount of connections to other nodes that need to be maintained by a single node.
 *
 *
 *                                          n n n n n
 *                                     n        |        n
 *                                     n        B        n
 *                                     n ---- B   B ---- n
 *                                     n        B        n
 *                                     n        |        n
 *                                          n n n n n
 *
 *
 * Broadcasting works as follows:
 * 1. A node that wants to broadcast a message sends it to it broadcaster
 * 2. The broadcaster forwards the message to all other broadcasters and to all other of its broadcastees
 * 3. Each broadcaster receiving a message forwards it to all of its broadcastees
 *
 * Additionally, fast paxos vote messages are batched together at two levels:
 * - when first received by the broadcasting node of a group of "broadcastees"
 * - when the broadcasted messages are subsequently received by the other broadcasters
 *
 * Broadcasters are discovered automatically via node meta-data. A node acts as broadcaster if configured to do so
 * via the Settings.
 *
 * /!\ NOTE: This broadcasting strategy does **not preserve the order of messages**.
 *
 */
@ExperimentalApi
public class ConsistentHashBroadcaster implements IBroadcaster {

    static final int CONSISTENT_HASH_REPLICAS = 200;

    private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashBroadcaster.class);

    private final boolean isBroadcaster;
    private final IMessagingClient messagingClient;
    private final MembershipView membershipView;
    private final Endpoint myAddress;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @GuardedBy("rwLock") private Set<Endpoint> allRecipients = new HashSet<>();
    @GuardedBy("rwLock") private Set<Endpoint> myRecipients = new HashSet<>();

    private ConsistentHash<Endpoint> broadcasterRing =
            new ConsistentHash<>(CONSISTENT_HASH_REPLICAS, Collections.emptyList());
    private Set<Endpoint> allBroadcasters = new HashSet<>();

    private final MessageBatcher<FastRoundPhase2bMessage> broadcasterMessageBatcher;
    private final MessageBatcher<FastRoundPhase2bMessage> recipientMessageBatcher;

    public ConsistentHashBroadcaster(final IMessagingClient messagingClient,
                                     final MembershipView membershipView,
                                     final Endpoint myAddress,
                                     final Optional<Metadata> myMetadata,
                                     final SharedResources sharedResources,
                                     final int consensusBatchingWindowInMs) {
        this.messagingClient = messagingClient;
        this.membershipView = membershipView;
        this.myAddress = myAddress;

        this.isBroadcaster = myMetadata.isPresent() && isBroadcasterNode(myMetadata.get());

        this.broadcasterMessageBatcher = new BroadcasterMessageBatcher(sharedResources, consensusBatchingWindowInMs);
        this.recipientMessageBatcher = new RecipientMessageBatcher(sharedResources, consensusBatchingWindowInMs);

        // Schedule fast paxos job
        if (isBroadcaster) {
            broadcasterMessageBatcher.start();
            recipientMessageBatcher.start();
        }
    }

    @Override
    @CanIgnoreReturnValue
    public List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest rapidRequest,
                                                           final long configurationId) {
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
        if (!broadcasterRing.isEmpty()) {
            final BroadcastingMessage.Builder broadcastingMessageBuilder = BroadcastingMessage.newBuilder()
                            .setMessage(rapidRequest)
                            .setConfigurationId(configurationId);

            if (isBroadcaster) {
                // directly send the message to other broadcasters for retransmission
                sendToBroadcastersAndRecipients(broadcastingMessageBuilder.setShouldDeliver(true).build());
            } else {
                if (rapidRequest.getContentCase().equals(RapidRequest.ContentCase.BATCHEDALERTMESSAGE)) {
                    // deliver to ourselves first
                    // at scale, this matters, as we may otherwise receive consensus messages (while still broadcasting)
                    // before having processed the batched alert message
                    final ListenableFuture<RapidResponse> myResponse =
                            messagingClient.sendMessage(myAddress, rapidRequest);
                    responses.add(myResponse);
                    Futures.addCallback(myResponse,
                            new ErrorReportingCallback(rapidRequest.getContentCase(), myAddress));
                }

                // send it to our broadcaster who will take care of retransmission
                final RapidRequest request = Utils.toRapidRequest(
                        broadcastingMessageBuilder.setShouldDeliver(false).build()
                );
                final Endpoint broadcaster = broadcasterRing.get(myAddress);
                final ListenableFuture<RapidResponse> response = messagingClient.sendMessage(broadcaster, request);
                Futures.addCallback(response, new ErrorReportingCallback(request.getContentCase(), broadcaster));
                responses.add(response);
            }
        } else {
            // fall back to direct broadcasting
            // this happens e.g. when the seed node joins
            allRecipients.forEach(node -> {
                responses.add(messagingClient.sendMessageBestEffort(node, rapidRequest));
            });

        }
        return responses;
    }

    @Override
    public void onNodeAdded(final Endpoint node, final Optional<Metadata> metadata) {
        rwLock.writeLock().lock();
        try {
            allRecipients.add(node);
            final boolean addedNodeIsBroadcaster = metadata.isPresent() && isBroadcasterNode(metadata.get());
            if (!allBroadcasters.contains(node) && addedNodeIsBroadcaster) {
                broadcasterRing.add(node);
                allBroadcasters.add(node);
            }
            if (isBroadcaster) {
                final Endpoint responsibleBroadcaster = broadcasterRing.get(node);
                if (myAddress.equals(responsibleBroadcaster)) {
                    myRecipients.add(node);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void onNodeRemoved(final Endpoint node) {
        rwLock.writeLock().lock();
        try {
            allRecipients.remove(node);
            broadcasterRing.remove(node);
            allBroadcasters.remove(node);
            myRecipients.remove(node);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public ListenableFuture<RapidResponse> handleBroadcastingMessage(final BroadcastingMessage msg) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();
        if (msg.getShouldDeliver()) {
            // this message already went through the ring, deliver to our recipients
            sendToOwnRecipients(msg.getMessage(), true);
        } else {
            // pass it on to all other broadcasters, and deliver to our recipients
            sendToBroadcastersAndRecipients(msg.toBuilder().setShouldDeliver(true).build());
        }
        future.set(null);
        return future;
    }

    private boolean isBroadcasterNode(final Metadata nodeMetadata) {
        return nodeMetadata.getMetadataCount() > 0
                && nodeMetadata.getMetadataMap().containsKey(Cluster.BROADCASTER_METADATA_KEY);
    }

    private void sendToOwnRecipients(final RapidRequest msg, final boolean shouldBatch) {
        if (shouldBatch && msg.getContentCase() == RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE) {
            recipientMessageBatcher.enqueueMessage(msg.getFastRoundPhase2BMessage());
        } else {
            try {
                rwLock.readLock().lock();
                // send to myself first
                Futures.addCallback(messagingClient.sendMessage(myAddress, msg),
                        new ErrorReportingCallback(msg.getContentCase(), myAddress));

                // then to the rest

                // except for when it's a batched alert message, we also do not send it to the sender
                final Set<Endpoint> recipientsToSkip = new HashSet<>();
                recipientsToSkip.add(myAddress);
                if (msg.getContentCase().equals(RapidRequest.ContentCase.BATCHEDALERTMESSAGE)) {
                    recipientsToSkip.add(msg.getBatchedAlertMessage().getSender());
                }

                myRecipients.forEach(node -> {
                    if (!recipientsToSkip.contains(node)) {
                        Futures.addCallback(messagingClient.sendMessage(node, msg),
                                new ErrorReportingCallback(msg.getContentCase(), myAddress));
                    }
                });
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    @CanIgnoreReturnValue
    private ListenableFuture<?> sendToBroadcastersAndRecipients(final BroadcastingMessage broadcastingMessage) {
        // first send to the broadcasters and only then to the recipients
        final ListenableFuture<List<RapidResponse>> broadcasterResponses =
                Futures.successfulAsList(sendToBroadcasters(broadcastingMessage, true));

        return Futures.transform(broadcasterResponses, rapidResponses -> {
            if (!broadcastingMessage.getMessage().getContentCase()
			.equals(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE)
                    && rapidResponses.size() != allBroadcasters.size() - 1) {
                LOG.warn("Could not deliver request of type {} to all broadcasters!",
                        broadcastingMessage.getMessage().getContentCase().name());
            }
            sendToOwnRecipients(broadcastingMessage.getMessage(), true);
            return rapidResponses;
        });
    }

    private List<ListenableFuture<RapidResponse>> sendToBroadcasters(final BroadcastingMessage message,
                                                                     final boolean shouldBatch) {
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
        if (shouldBatch &&
                message.getMessage().getContentCase().equals(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE)) {
            broadcasterMessageBatcher.enqueueMessage(message.getMessage().getFastRoundPhase2BMessage());
        } else {
            rwLock.readLock().lock();
            try {
                allBroadcasters.stream().filter(node -> !node.equals(myAddress)).forEach(node -> {
                    final ListenableFuture<RapidResponse> response =
                            messagingClient.sendMessage(node, Utils.toRapidRequest(message));
                    Futures.addCallback(response,
                            new ErrorReportingCallback(message.getMessage().getContentCase(), node));
                    responses.add(response);
                });
            } finally {
                rwLock.readLock().unlock();
            }
        }
        return responses;
    }

    private FastRoundPhase2bMessage compressConsensusMessages(final List<FastRoundPhase2bMessage> messages) {
        // compress all the messages into one
        final Map<List<Endpoint>, BitSet> allProposals = new HashMap<>();
        final Map<List<Endpoint>, Long> allProposalConfigurationIds = new HashMap<>();
        for (final FastRoundPhase2bMessage consensusMessage : messages) {
            for (final Proposal proposal : consensusMessage.getProposalsList()) {
                final BitSet allVotes = allProposals.computeIfAbsent(proposal.getEndpointsList(),
                        endpoints -> BitSet.valueOf(proposal.getVotes().toByteArray()));
                allVotes.or(BitSet.valueOf(proposal.getVotes().toByteArray()));
                allProposals.put(proposal.getEndpointsList(), allVotes);
                allProposalConfigurationIds.put(proposal.getEndpointsList(), proposal.getConfigurationId());
            }
        }
        final List<Proposal> allProposalMessages = new ArrayList<>();
        allProposals.entrySet().forEach(entry -> {
            final Proposal proposal = Proposal.newBuilder()
                    // configuration IDs should be stable for proposals affecting the same nodes
                    .setConfigurationId(allProposalConfigurationIds.get(entry.getKey()))
                    .addAllEndpoints(entry.getKey())
                    .setVotes(ByteString.copyFrom(entry.getValue().toByteArray()))
                    .build();
            allProposalMessages.add(proposal);
        });
        final FastRoundPhase2bMessage batched = FastRoundPhase2bMessage.newBuilder()
                .addAllProposals(allProposalMessages)
                .build();
        return batched;
    }

    private class BroadcasterMessageBatcher extends MessageBatcher<FastRoundPhase2bMessage> {
        public BroadcasterMessageBatcher(final SharedResources sharedResources, final int batchingWindowInMs) {
            super(sharedResources, batchingWindowInMs);
        }

        @Override
        public void sendMessages(final List<FastRoundPhase2bMessage> messages) {
            final FastRoundPhase2bMessage batched = compressConsensusMessages(messages);
            final BroadcastingMessage message = BroadcastingMessage.newBuilder()
                    .setMessage(Utils.toRapidRequest(batched))
                    .setShouldDeliver(true)
                    .setConfigurationId(membershipView.getCurrentConfigurationId())
                    .build();
            sendToBroadcasters(message, false);
        }
    }

    private class RecipientMessageBatcher extends MessageBatcher<FastRoundPhase2bMessage> {
        public RecipientMessageBatcher(final SharedResources sharedResources, final int batchingWindowInMs) {
            super(sharedResources, batchingWindowInMs);
        }

        @Override
        public void sendMessages(final List<FastRoundPhase2bMessage> messages) {
            final FastRoundPhase2bMessage batched = compressConsensusMessages(messages);
            sendToOwnRecipients(Utils.toRapidRequest(batched), false);
        }
    }

    private static class ErrorReportingCallback implements FutureCallback<RapidResponse> {
        private final RapidRequest.ContentCase requestType;
        private final Endpoint recipient;

        public ErrorReportingCallback(final RapidRequest.ContentCase requestType, final Endpoint recipient) {
            this.requestType = requestType;
            this.recipient = recipient;
        }

        private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashBroadcaster.class);

        @Override
        public void onSuccess(@Nullable final RapidResponse rapidResponse) {
        }

        @Override
        public void onFailure(final Throwable throwable) {
            LOG.warn("Failed to broadcast request of type {} to {}:{}",
                    requestType.name(), recipient.getHostname().toStringUtf8(), recipient.getPort());
        }
    }

}
