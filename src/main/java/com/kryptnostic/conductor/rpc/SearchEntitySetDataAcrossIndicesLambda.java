package com.kryptnostic.conductor.rpc;

import com.dataloom.data.EntityKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class SearchEntitySetDataAcrossIndicesLambda
        implements Function<ConductorElasticsearchApi, List<EntityKey>>, Serializable {

    private static final long serialVersionUID = 874720830583573161L;

    private Map<UUID, UUID>               entitySetAndSyncIds;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private Map<UUID, DelegatedStringSet> fieldSearches;
    private int                           size;
    private boolean                       explain;

    public SearchEntitySetDataAcrossIndicesLambda(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        this.entitySetAndSyncIds = entitySetAndSyncIds;
        this.fieldSearches = fieldSearches;
        this.size = size;
        this.explain = explain;
    }

    @Override
    public List<EntityKey> apply( ConductorElasticsearchApi api ) {
        return api.executeEntitySetDataSearchAcrossIndices( entitySetAndSyncIds, fieldSearches, size, explain );
    }
}
