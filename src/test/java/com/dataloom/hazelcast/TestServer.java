package com.dataloom.hazelcast;

import com.kryptnostic.rhizome.core.RhizomeApplicationServer;

public class TestServer extends RhizomeApplicationServer {
    public TestServer( Class<?>... pods) {
        super( pods );
    }
}
