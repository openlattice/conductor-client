package com.dataloom.linking;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Multimap;

public interface ConductorLinkingApi {

    public UUID link( Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties );
   
}
