package org.pentaho.di.trans.steps.arrowflight;


import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.util.Objects.requireNonNull;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import org.apache.arrow.dataset.jni.NativeMemoryPool;

public class UseCases {

    //use case 1: ArrowClient connect to Server, list flights
    public static void useCase1() {
        BufferAllocator allocator = new RootAllocator();
        ApacheFlightConnection connection = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);

        System.out.println("Connected to server for use case 1, client: " + connection.getClient());

        connection.listFlightInfos();
    }

    //use case 2: ArrowClient connect to Server, gets flight_info for path "myfile01.csv", prints flight_info.descriptor.path on console
    public static void useCase2() {
        BufferAllocator allocator = new RootAllocator();
        ApacheFlightConnection connection = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);

        System.out.println("Connected to server for use case 2, client: " + connection.getClient());


        FlightInfo info = connection.getFlightInfo("profiles");
        FlightInfo more_info = connection.getFlightInfo("more_profiles");

        System.out.println("Descriptor path: " + info.getDescriptor().getPath());
        System.out.println("Descriptor path: " + more_info.getDescriptor().getPath());


    }

    //use case 3: ArrowClient connect to Server, gets flight_info for path "myfile01.csv", gets all record-batches and dumps all data to console
    public static void useCase3() {
        BufferAllocator allocator = new RootAllocator();
        ApacheFlightConnection connection = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);

        System.out.println("Connected to server for use case 3, client: " + connection.getClient());



        FlightInfo info = connection.getFlightInfo("profiles");

        FlightInfo more_info = connection.getFlightInfo("more_profiles");


        System.out.println("====DISPLAYING FIRST STREAM====");
        connection.showFlightData(info.getDescriptor().getPath().get(0));
        System.out.println("====DISPLAYING SECOND STREAM====");
        connection.showFlightData(more_info.getDescriptor().getPath().get(0));


    }

    public static void useCase4() {

        BufferAllocator allocator = new RootAllocator();
        ApacheFlightConnection connection = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);

        System.out.println("Connected to server for use case 4, client: " + connection.getClient());



        FlightInfo info = connection.getFlightInfo("profiles");
        FlightInfo more_info = connection.getFlightInfo("more_profiles");

        FlightStream info_stream = connection.getFlightStream(info.getDescriptor().getPath().get(0));
        FlightStream more_info_stream = connection.getFlightStream(more_info.getDescriptor().getPath().get(0));

        ArrayList<Object[]> data = connection.getFlightData(info_stream);
        ArrayList<Object[]> more_data = connection.getFlightData(more_info_stream);

        //System.out.println(more_info_stream.getDescriptor());
        System.out.println(more_info_stream.getSchema());

        connection.printFlightDataConsole(data);
        connection.printFlightDataConsole(more_data);
    }

    public static  void main(String[] args) {

        useCase1();
        System.out.println("");
        useCase2();
        System.out.println("");
        useCase3();
        System.out.println("");
        useCase4();
    }
}
