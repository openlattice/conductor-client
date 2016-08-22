package com.kryptnostic.conductor.rpc;

import java.util.UUID;

public class UUIDs {
    public static final UUID ANONYMOUS = new UUID( 0, 0 );

    public static enum Syncs {
        BASE( new UUID( 0, 0 ) );
        private final UUID syncId;

        private Syncs( UUID syncId ) {
            this.syncId = syncId;
        }

        public UUID getSyncId() {
            return syncId;
        }
    }

    public static final class ACLs {
        public static final UUID EVERYONE_ACL = new UUID( 0, 0 );
    }
}
