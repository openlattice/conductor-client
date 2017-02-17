package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.spark_project.guava.collect.Maps;

import com.dataloom.search.requests.SearchResult;
import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class SearchResultStreamSerializerTest extends AbstractStreamSerializerTest<SearchResultStreamSerializer, SearchResult>
implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 8177027116300219868L;

    @Override
    protected SearchResultStreamSerializer createSerializer() {
        return new SearchResultStreamSerializer();
    }

    @Override
    protected SearchResult createInput() {
        Map<String, Object> firstHit = Maps.newHashMap();
        Map<String, Object> secondHit = Maps.newHashMap();
        firstHit.put( "color", "green" );
        firstHit.put( "size", "seven" );
        secondHit.put( "color", "blue" );
        secondHit.put( "size", "four" );
        List<Map<String, Object>> hits = Lists.newArrayList();
        hits.add( firstHit );
        hits.add( secondHit );
        return new SearchResult( Long.valueOf( "2" ), hits );
    }

}
