package org.pentaho.di.trans.steps.arrowflight;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;

import static java.util.Arrays.asList;

public class populateServer {

    public static void main(String[] args) throws Exception {
        BufferAllocator allocator = new RootAllocator();
        ApacheFlightConnection connect = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);
        System.out.println("C1: Client (Location): Connected to " + connect.getLocation().getUri());

        // Populate data
        //TODO schema pode ser coded para algo global a classe, portanto devo poder faze uma funcao de populate i guess?
        Schema schema = new Schema(Arrays.asList(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
            VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
            varCharVector.allocateNew(3);
            varCharVector.set(0, "Ronald".getBytes());
            varCharVector.set(1, "David".getBytes());
            varCharVector.set(2, "Francisco".getBytes());
            vectorSchemaRoot.setRowCount(3);
            FlightClient.ClientStreamListener listener = connect.getClient().startPut(
                    FlightDescriptor.path("profiles"),
                    vectorSchemaRoot, new AsyncPutListener());
            listener.putNext();
            varCharVector.set(0, "Manuel".getBytes());
            varCharVector.set(1, "Felipe".getBytes());
            varCharVector.set(2, "JJ".getBytes());
            vectorSchemaRoot.setRowCount(3);
            listener.putNext();
            listener.completed();
            listener.getResult();
            System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
        }

        System.out.println("----------SHOWING RECEIVED DATA------------");
        //TODO passar isto para funcao
        connect.showFlightData("profiles");

        try (BufferAllocator rootAllocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try(
                    VectorSchemaRoot vectorSchemaRoot2 = VectorSchemaRoot.create(schemaPerson, rootAllocator)
            ){
                VarCharVector nameVector = (VarCharVector) vectorSchemaRoot2.getVector("name");
                nameVector.allocateNew(3);
                nameVector.set(0, "David".getBytes());
                nameVector.set(1, "Gladis".getBytes());
                nameVector.set(2, "Juan".getBytes());
                IntVector ageVector = (IntVector) vectorSchemaRoot2.getVector("age");
                ageVector.allocateNew(3);
                ageVector.set(0, 10);
                ageVector.set(1, 20);
                ageVector.set(2, 30);
                vectorSchemaRoot2.setRowCount(3);

                FlightClient.ClientStreamListener listener2 = connect.getClient().startPut(
                        FlightDescriptor.path("more_profiles"),
                        vectorSchemaRoot2, new AsyncPutListener());

                listener2.putNext();
                listener2.completed();
                listener2.getResult();
                try (
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot2, null, Channels.newChannel(out))
                ){
                    writer.start();
                    writer.writeBatch();
                    System.out.println("Number of rows written: " + vectorSchemaRoot2.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("acabou wewewewewewewewewewe");


        // Get metadata information
        FlightInfo flightInfo = connect.getFlightInfo("profiles");
        System.out.println("C3: Client (Get Metadata): " + flightInfo);

        System.out.println("aaaaaaaaaa");
        connect.showFlightData("profiles");
        connect.showFlightData("more_profiles");

        // Get all metadata information
        connect.listFlightInfos();
    }
}
