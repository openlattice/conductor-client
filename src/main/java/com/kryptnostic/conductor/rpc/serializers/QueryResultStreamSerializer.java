package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.kryptnostic.services.v1.serialization.UUIDStreamSerializer;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;

public class QueryResultStreamSerializer implements SelfRegisteringStreamSerializer<QueryResult> {
    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            // Stuff from
            // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
            kryo.setInstantiatorStrategy( new Kryo.DefaultInstantiatorStrategy( new StdInstantiatorStrategy() ) );
            kryo.register( Object[].class );
            kryo.register( java.lang.Class.class );

            // Shared Lambdas
            kryo.register( Lambdas.class );
            kryo.register( SerializedLambda.class );

            // always needed for closure serialization, also if registrationRequired=false
            kryo.register( ClosureSerializer.Closure.class, new ClosureSerializer() );

            kryo.register( Callable.class, new ClosureSerializer() );

            return kryo;
        }
    };
        
	@Override
	public void write(ObjectDataOutput out, QueryResult object) throws IOException {
		out.writeUTF( object.getTableName() );
		out.writeUTF( object.getKeyspace() );
		UUIDStreamSerializer.serialize( out, object.getQueryId() );
		out.writeUTF( object.getSessionId() );
		// serialize Object.getEntitySet()
		
//		try {
//			ODataSerializer od = OData.newInstance().createSerializer( ContentType.TEXT_PLAIN );
//		//	SerializerResult result = od.entityCollection(object.getEntitySet()., entityType, object.getEntitySet(), options)
//		} catch (SerializerException e) {
//			e.printStackTrace();
//		}
		
		Output output = new Output( (OutputStream) out );
		kryoThreadLocal.get().writeClassAndObject(output, object);
	}

	@Override
	public QueryResult read(ObjectDataInput in) throws IOException {
		String tableName = in.readUTF();
		String keyspace = in.readUTF();
		UUID queryId = UUIDStreamSerializer.deserialize( in );
		String sessionId = in.readUTF();
		// deserialize entity set
		CsdlEntitySet es = null; // remove this
		Input input = new Input( (InputStream) in );
		Callable<?> c = ( Callable<?> ) kryoThreadLocal.get().readClass( input );
		return new QueryResult( tableName, keyspace, queryId, sessionId, es );
	}

	@Override
	public int getTypeId() {
		return HazelcastSerializerTypeIds.QUERY_RESULT.ordinal();
	}

	@Override
	public void destroy() {
		
	}

	@Override
	public Class<QueryResult> getClazz() {
		return QueryResult.class;
	}

}

