package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class EdgeSelection {
    private final UUID graphId;
    private final Optional<UUID> optionalSrcId;
    private final Optional<UUID> optionalSrcType;
    private final Optional<UUID> optionalDstId;
    private final Optional<UUID> optionalDstType;
    private final Optional<UUID> optionalEdgeType;

    public EdgeSelection(
            UUID graphId,
            Optional<UUID> optionalSrcId,
            Optional<UUID> optionalSrcType,
            Optional<UUID> optionalDstId,
            Optional<UUID> optionalDstType,
            Optional<UUID> optionalEdgeType ) {
        Preconditions.checkNotNull( graphId, "Graph Id must be provided." );
        Preconditions.checkArgument(
                optionalSrcId.isPresent() || optionalSrcType.isPresent() || optionalDstId.isPresent()
                        || optionalDstType.isPresent() || optionalEdgeType.isPresent(),
                "You cannot run an empty edge selection query. At least one parameter must be specified." );
        this.graphId = graphId;
        this.optionalSrcId = optionalSrcId;
        this.optionalSrcType = optionalSrcType;
        this.optionalDstId = optionalDstId;
        this.optionalDstType = optionalDstType;
        this.optionalEdgeType = optionalEdgeType;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Optional<UUID> getOptionalSrcId() {
        return optionalSrcId;
    }

    public Optional<UUID> getOptionalSrcType() {
        return optionalSrcType;
    }

    public Optional<UUID> getOptionalDstId() {
        return optionalDstId;
    }

    public Optional<UUID> getOptionalDstType() {
        return optionalDstType;
    }

    public Optional<UUID> getOptionalEdgeType() {
        return optionalEdgeType;
    }

}
