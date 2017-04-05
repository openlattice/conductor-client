package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.dataloom.search.requests.SearchResult;

public class AdvancedSearchEntitySetDataLambda
        implements Function<ConductorElasticsearchApi, SearchResult>, Serializable {
    private static final long serialVersionUID = 6807941698843957059L;

    private UUID              entitySetId;
    private UUID              syncId;
    private Map<UUID, String> searches;
    private int               start;
    private int               maxHits;
    private Set<UUID>         authorizedPropertyTypes;

    public AdvancedSearchEntitySetDataLambda(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, String> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.searches = searches;
        this.start = start;
        this.maxHits = maxHits;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
    }

    @Override
    public SearchResult apply( ConductorElasticsearchApi api ) {
        return api.executeAdvancedEntitySetDataSearch( entitySetId,
                syncId,
                searches,
                start,
                maxHits,
                authorizedPropertyTypes );
    }

}
