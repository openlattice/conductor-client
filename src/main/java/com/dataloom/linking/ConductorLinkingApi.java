package com.dataloom.linking;

import com.google.common.collect.Multimap;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface ConductorLinkingApi {

    UUID link( Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties );

}
