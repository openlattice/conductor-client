package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class SearchEntitySetDataLambda implements Function<ConductorSparkApi, List<Map<String, Object>>>, Serializable {
    private static final long serialVersionUID = -3273005291047567056L;

    private UUID entitySetId;
    private String searchTerm;
    private Set<UUID> authorizedProperties;
    
    public SearchEntitySetDataLambda( UUID entitySetId, String searchTerm, Set<UUID> authorizedProperties) {
        this.entitySetId = entitySetId;
        this.searchTerm = searchTerm;
        this.authorizedProperties = authorizedProperties;
    }
    
    @Override
    public List<Map<String, Object>> apply( ConductorSparkApi api ) {
        return api.executeEntitySetDataSearch( entitySetId, searchTerm, authorizedProperties );
    }

}
