package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;
import java.util.concurrent.Callable;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@SuppressWarnings( "rawtypes" )
public class CallableStreamSerializer implements SelfRegisteringStreamSerializer<Callable> {
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

            kryo.register( Runnable.class, new ClosureSerializer() );

            return kryo;
        }
    };

    public CallableStreamSerializer() {}

    @Override
    public void write( ObjectDataOutput out, Callable object ) throws IOException {
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object );
        output.flush();
    }

    @Override
    public Callable read( ObjectDataInput in ) throws IOException {
        Input input = new Input( (InputStream) in );
        return (Callable) kryoThreadLocal.get().readClassAndObject( input );
    }

    @Override
    public int getTypeId() {
        return HazelcastSerializerTypeIds.CALLABLE.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Callable> getClazz() {
        return Callable.class;
    }
}
