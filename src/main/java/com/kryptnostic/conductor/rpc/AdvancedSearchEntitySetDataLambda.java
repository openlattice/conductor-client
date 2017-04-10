package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.dataloom.search.requests.SearchDetails;
import com.dataloom.search.requests.SearchResult;

public class AdvancedSearchEntitySetDataLambda implements Function<ConductorElasticsearchApi, SearchResult>, Serializable {
    private static final long serialVersionUID = -7485362089184143910L;

    private UUID entitySetId;
    private List<SearchDetails> searches;
    private int start;
    private int maxHits;
    private Set<UUID> authorizedPropertyTypes;
    
    public AdvancedSearchEntitySetDataLambda( UUID entitySetId, List<SearchDetails> searches, int start, int maxHits, Set<UUID> authorizedPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.searches = searches;
        this.start = start;
        this.maxHits = maxHits;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
    }
    
    @Override
    public SearchResult apply( ConductorElasticsearchApi api ) {
        return api.executeAdvancedEntitySetDataSearch( entitySetId, searches, start, maxHits, authorizedPropertyTypes );
    }

}
