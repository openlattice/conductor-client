package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.inject.Inject;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class ConductorCallStreamSerializer implements SelfRegisteringStreamSerializer<ConductorCall> {

    private final ConductorSparkApi api;
    private final Kryo              kryo = new Kryo();

    @Inject
    public ConductorCallStreamSerializer( ConductorSparkApi api ) {
        this.api = api;
        kryo.register( ConductorCall.class );
    }

    @Override
    public void write( ObjectDataOutput out, ConductorCall object ) throws IOException {
        // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // ObjectOutput oo = new ObjectOutputStream( baos );
        // oo.writeObject( object );
        // oo.flush();
        // out.writeByteArray( baos.toByteArray() );
        Output output = new Output( (OutputStream) out );
        kryo.writeClassAndObject( output, object );
        output.flush();
    }

    @Override
    public ConductorCall read( ObjectDataInput in ) throws IOException {
        // byte[] b = in.readByteArray();
        // ObjectInput input = new ObjectInputStream( new ByteArrayInputStream( b ) );
        // ConductorCall c;
        // try {
        // c = (ConductorCall) input.readObject();
        // } catch ( ClassNotFoundException e ) {
        // e.printStackTrace();
        // return null;
        // }
        // c.setApi( api );
        // return c;

        Input input = new Input( (InputStream) in );
        ConductorCall c = (ConductorCall) kryo.readClassAndObject( input );
        c.setApi( api );
        return c;
    }

    @Override
    public int getTypeId() {
        return HazelcastSerializerTypeIds.CONDUCTOR_CALL.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<ConductorCall> getClazz() {
        return ConductorCall.class;
    }

}
