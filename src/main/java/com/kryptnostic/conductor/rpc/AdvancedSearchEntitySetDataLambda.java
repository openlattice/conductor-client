package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.dataloom.search.requests.SearchResult;

public class AdvancedSearchEntitySetDataLambda implements Function<ConductorSparkApi, SearchResult>, Serializable {
    private static final long serialVersionUID = 6807941698843957059L;

    private UUID entitySetId;
    private Map<UUID, String> searches;
    private int start;
    private int maxHits;
    private Set<UUID> authorizedPropertyTypes;
    
    public AdvancedSearchEntitySetDataLambda( UUID entitySetId, Map<UUID, String> searches, int start, int maxHits, Set<UUID> authorizedPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.searches = searches;
        this.start = start;
        this.maxHits = maxHits;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
    }
    
    @Override
    public SearchResult apply( ConductorSparkApi api ) {
        return api.executeAdvancedEntitySetDataSearch( entitySetId, searches, start, maxHits, authorizedPropertyTypes );
    }

}
