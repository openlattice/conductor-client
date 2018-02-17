package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.mappers.ObjectMappers;
import com.openlattice.search.requests.SearchResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class SearchResultStreamSerializer implements SelfRegisteringStreamSerializer<SearchResult> {
    
    private static final TypeReference<List<Map<String, Object>>> hitType = new TypeReference<List<Map<String,Object>>>(){};

    @Override
    public void write( ObjectDataOutput out, SearchResult object ) throws IOException {
        out.writeLong( object.getNumHits() );
        out.writeByteArray( ObjectMappers.getSmileMapper().writeValueAsBytes( object.getHits() ) );
    }

    @Override
    public SearchResult read( ObjectDataInput in ) throws IOException {
        long numHits = in.readLong();
        List<Map<String, Object>> hits = ObjectMappers.getSmileMapper().readValue( in.readByteArray(), hitType );
        return new SearchResult( numHits, hits );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.SEARCH_RESULT.ordinal();
    }

    @Override
    public void destroy() {
                
    }

    @Override
    public Class<? extends SearchResult> getClazz() {
        return SearchResult.class;
    }
    

}
