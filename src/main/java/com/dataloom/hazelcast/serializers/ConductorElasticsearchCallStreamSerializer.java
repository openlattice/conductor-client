package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.Organization;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.*;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.serializers.AclKeyKryoSerializer;
import com.openlattice.authorization.serializers.EntityDataLambdasStreamSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;
import java.util.UUID;
import java.util.function.Function;

@SuppressWarnings( "rawtypes" )
@Component
public class ConductorElasticsearchCallStreamSerializer
        implements SelfRegisteringStreamSerializer<ConductorElasticsearchCall> {
    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();

            // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
            kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(
                            new StdInstantiatorStrategy() ) );
            kryo.register( Object[].class );
            kryo.register( java.lang.Class.class );
            kryo.register( Organization.class );
            kryo.register( UUID.class );
            kryo.register( SetMultimap.class, new HashMultimapSerializer() );
            // Shared Lambdas
            kryo.register( ElasticsearchLambdas.class );
            kryo.register( EntityDataLambdas.class, new EntityDataLambdasStreamSerializer() );
            kryo.register( SearchEntitySetDataLambda.class );
            kryo.register( SerializedLambda.class );
            kryo.register( AclKey.class, new AclKeyKryoSerializer() );

            // always needed for closure serialization, also if
            // registrationRequired=false
            kryo.register( ClosureSerializer.Closure.class,
                    new ClosureSerializer() );
            kryo.register( Function.class,
                    new ClosureSerializer() );

            kryo.register( AclKey.class, new AclKeyKryoSerializer() );

            return kryo;
        }
    };

    private ConductorElasticsearchApi api;

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, ConductorElasticsearchCall object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getUserId() );
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object.getFunction() );
        output.flush();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    @SuppressFBWarnings
    public ConductorElasticsearchCall read( ObjectDataInput in ) throws IOException {
        UUID userId = UUIDStreamSerializer.deserialize( in );
        Input input = new Input( (InputStream) in );
        Function<ConductorElasticsearchApi, ?> f = (Function<ConductorElasticsearchApi, ?>) kryoThreadLocal.get()
                .readClassAndObject( input );
        return new ConductorElasticsearchCall( userId, f, api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.CONDUCTOR_ELASTICSEARCH_CALL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

    @Override
    public Class<? extends ConductorElasticsearchCall> getClazz() {
        return ConductorElasticsearchCall.class;
    }

}
