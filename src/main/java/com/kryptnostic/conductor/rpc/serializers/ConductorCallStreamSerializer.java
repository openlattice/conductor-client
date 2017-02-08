package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.EntityDataLambdas;
import com.kryptnostic.conductor.rpc.GetAllEntitiesOfTypeLambda;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.SearchEntitySetDataLambda;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
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
public class ConductorCallStreamSerializer implements SelfRegisteringStreamSerializer<ConductorCall> {
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

            // Shared Lambdas
            kryo.register( Lambdas.class );
            kryo.register( GetAllEntitiesOfTypeLambda.class );
            kryo.register( EntityDataLambdas.class );
            kryo.register( SearchEntitySetDataLambda.class );
            kryo.register( SerializedLambda.class );

            // always needed for closure serialization, also if
            // registrationRequired=false
            kryo.register( ClosureSerializer.Closure.class,
                    new ClosureSerializer() );

            kryo.register( Function.class,
                    new ClosureSerializer() );

            return kryo;
        }
    };

    private ConductorSparkApi api;

    @Override
    public void write( ObjectDataOutput out, ConductorCall object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getUserId() );
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object.getFunction() );
        output.flush();
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public ConductorCall read( ObjectDataInput in ) throws IOException {
        UUID userId = UUIDStreamSerializer.deserialize( in );
        Input input = new Input( (InputStream) in );
        Function<ConductorSparkApi, ?> f = (Function<ConductorSparkApi, ?>) kryoThreadLocal.get()
                .readClassAndObject( input );
        return new ConductorCall( userId, f, api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.CONDUCTOR_CALL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public synchronized void setConductorSparkApi( ConductorSparkApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

    @Override
    public Class<ConductorCall> getClazz() {
        return ConductorCall.class;
    }

}
