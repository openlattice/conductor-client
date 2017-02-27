package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.dataloom.linking.Entity;

public class SearchEntitySetDataAcrossIndicesLambda
        implements Function<ConductorElasticsearchApi, List<Entity>>, Serializable {

    private static final long      serialVersionUID = 874720830583573161L;

    private Set<UUID>              entitySetIds;
    private Map<UUID, Set<String>> fieldSearches;
    private int                    size;
    private boolean                explain;

    public SearchEntitySetDataAcrossIndicesLambda(
            Set<UUID> entitySetIds,
            Map<UUID, Set<String>> fieldSearches,
            int size,
            boolean explain ) {
        this.entitySetIds = entitySetIds;
        this.fieldSearches = fieldSearches;
        this.size = size;
        this.explain = explain;
    }

    @Override
    public List<Entity> apply( ConductorElasticsearchApi api ) {
        return api.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, size, explain );
    }
}
