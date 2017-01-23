package com.dataloom.authorization;

import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDList;

import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AclKey extends DelegatedUUIDList {
    public AclKey( List<UUID> uuids ) {
        super( uuids );
    }

    public static AclKey wrap( List<UUID> uuids ) {
        return new AclKey( uuids );
    }
}
