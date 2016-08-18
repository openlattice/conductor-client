package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class LambdaStreamSerializer implements SelfRegisteringStreamSerializer<Runnable> {
    private final Kryo kryo = new Kryo();

    public LambdaStreamSerializer() {
        kryo.register( Runnable.class, new ClosureSerializer() );
        kryo.setInstantiatorStrategy( new Kryo.DefaultInstantiatorStrategy( new StdInstantiatorStrategy() ) );
        kryo.register( Object[].class );
        kryo.register( java.lang.Class.class );
        kryo.register( getClass() ); // closure capturing class (in this test `this`), it would usually already be
                                     // registered
        kryo.register( SerializedLambda.class );
        // always needed for closure serialization, also if registrationRequired=false
        // kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    }

    @Override
    public void write( ObjectDataOutput out, Runnable object ) throws IOException {
        // Output output = new Output( (OutputStream) out );
        // kryo.writeClassAndObject( output, object );
        // output.flush();
        OutputStream os = (OutputStream) out;
        ObjectOutput oo = new ObjectOutputStream( os );
        oo.writeObject( object );
        oo.flush();
    }

    @Override
    public Runnable read( ObjectDataInput in ) throws IOException {
        // Input input = new Input( (InputStream) in );
        // return (Runnable) kryo.readClassAndObject( input );
        ObjectInput input = new ObjectInputStream( (InputStream) in );
        try {
            return (Runnable) input.readObject();
        } catch ( ClassNotFoundException e ) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public int getTypeId() {
        return HazelcastSerializerTypeIds.RUNNABLE.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Runnable> getClazz() {
        return Runnable.class;
    }

}
