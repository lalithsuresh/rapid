package com.vrg;


import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.UnreachableMember;


/**
 * Created by lsuresh on 3/31/17.
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
    public void onReceive(Object message) {
        if (message instanceof MemberUp) {
            MemberUp mUp = (MemberUp) message;

        } else if (message instanceof UnreachableMember) {
            UnreachableMember mUnreachable = (UnreachableMember) message;
            System.out.println(mUnreachable);

        } else if (message instanceof MemberRemoved) {
            MemberRemoved mRemoved = (MemberRemoved) message;

        } else if (message instanceof MemberEvent) {
            // ignore
        } else {
            unhandled(message);
        }

    }
}
