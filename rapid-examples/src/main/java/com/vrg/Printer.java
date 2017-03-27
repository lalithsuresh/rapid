package com.vrg;

import akka.actor.UntypedActor;

/**
 * Akka actors, distributed using Rapid
 */
public class Printer extends UntypedActor {

    @Override
    public void onReceive(final Object message) throws Throwable {
        if (message instanceof String) {
            System.out.println((String) message);
        }
    }
}
