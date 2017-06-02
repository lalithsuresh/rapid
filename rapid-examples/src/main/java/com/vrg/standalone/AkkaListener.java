package com.vrg.standalone;


import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.UnreachableMember;


/**
 * Akka Actor example
 */
public class AkkaListener extends UntypedActor {
    private final Cluster cluster = Cluster.get(getContext().system());

    //subscribe to cluster changes
    @Override
    public void preStart() {
        //#subscribe
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                MemberEvent.class, MemberUp.class, MemberRemoved.class, UnreachableMember.class);
        //#subscribe
    }

    //re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(final Object message) {
        if (message instanceof MemberUp) {
            final MemberUp upEvent = (MemberUp) message;
            System.out.println(upEvent);
        } else if (message instanceof UnreachableMember) {
            final UnreachableMember unreachableEvent = (UnreachableMember) message;
            System.out.println(unreachableEvent);

        } else if (message instanceof MemberRemoved) {
            final MemberRemoved removedEvent = (MemberRemoved) message;
            System.out.println(removedEvent);
        } else {
            unhandled(message);
        }

    }
}
