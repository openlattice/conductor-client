package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.openlattice.search.requests.SearchDetails;
import com.openlattice.search.requests.SearchResult;

public class AdvancedSearchEntitySetDataLambda
        implements Function<ConductorElasticsearchApi, SearchResult>, Serializable {
    private static final long serialVersionUID = 8549826561713602245L;

    private UUID                entitySetId;
    private UUID                syncId;
    private List<SearchDetails> searches;
    private int                 start;
    private int                 maxHits;
    private Set<UUID>           authorizedPropertyTypes;

    public AdvancedSearchEntitySetDataLambda(
            UUID entitySetId,
            UUID syncId,
            List<SearchDetails> searches,
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
