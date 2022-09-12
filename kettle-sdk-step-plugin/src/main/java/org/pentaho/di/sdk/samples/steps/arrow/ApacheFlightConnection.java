package org.pentaho.di.sdk.samples.steps.arrow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.util.AutoCloseables;


import org.apache.arrow.vector.ipc.ArrowStreamReader;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ApacheFlightConnection implements AutoCloseable {


    private FlightClient _client;
    private BufferAllocator _allocator;

    private Location _location;

    public ApacheFlightConnection(FlightClient client, BufferAllocator allocator, Location location) {
        _client = client;
        _allocator = allocator;
        _location = location;
    }

    public FlightInfo getFlightInfo(String path, CallOption... options) {
        return _client.getInfo(FlightDescriptor.path(path));
    }

    public ArrayList<Object[]> getFlightData(FlightStream flightStream) {
        int batch = 0;
        Schema schema = flightStream.getSchema();
        ArrayList<Object[]> table = new ArrayList<Object[]>();
        int rowLen = schema.getFields().size();

        Object[] header = new Object[rowLen];
        int row_index = 0;

        Iterator fields = schema.getFields().iterator();

        while (fields.hasNext()) {
            Field field = (Field) fields.next();
            header[row_index] = field.getName();
            row_index++;
        }

        table.add(header);

        row_index = 0;

        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
            while (flightStream.next()) {
                batch++;
                for (int i = 0; i < vectorSchemaRootReceived.getRowCount(); ++i) {
                    row_index = 0;
                    Object[] row = new Object[rowLen];

                    Iterator var7 = vectorSchemaRootReceived.getFieldVectors().iterator();

                    while (var7.hasNext()) {
                        FieldVector v = (FieldVector) var7.next();
                        row[row_index] = v.getObject(i);
                        row_index++;
                    }

                    table.add(row);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    public void printFlightDataConsole(ArrayList<Object[]> table) {
        for( int i = 0; i  < table.size(); i++) {
            System.out.println(Arrays.deepToString(table.get(i)));
        }

        System.out.println("");
    }

    public void showFlightData(String path) {
        try (FlightStream flightStream = getFlightStream(path)) {
            int batch = 0;
            Schema schema = flightStream.getSchema();
            System.out.println(schema.getFields());
            ArrayList<ArrayList<Object>> table = new ArrayList<ArrayList<Object>>();
            ArrayList<Object> row = new ArrayList<Object>(schema.getFields().size());

            Iterator fields = schema.getFields().iterator();

            while (fields.hasNext()) {
                Field field = (Field) fields.next();
                row.add(field.getName());
            }

            table.add(row);
            System.out.println(table);

            try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                while (flightStream.next()) {
                    batch++;
                    for (int i = 0; i < vectorSchemaRootReceived.getRowCount(); ++i) {
                        ArrayList<Object> linha = new ArrayList<Object>(schema.getFields().size());

                        Iterator var7 = vectorSchemaRootReceived.getFieldVectors().iterator();

                        while (var7.hasNext()) {
                            FieldVector v = (FieldVector) var7.next();
                            linha.add(v.getObject(i));
                        }

                        table.add(linha);
                    }
                }
                System.out.println(table);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public FlightClient getClient() { return _client; }
    public Location getLocation() { return _location; }

    public List<Field> getPDIFields(FlightStream stream) { return stream.getSchema().getFields(); }


    public FlightStream getFlightStream(String path, CallOption... options) {
        return _client.getStream(new Ticket(
                FlightDescriptor.path(path).getPath().get(0).getBytes(StandardCharsets.UTF_8)));
    }

    public static ApacheFlightConnection createFlightClient(BufferAllocator allocator, String host, int port) {
        Location location = Location.forGrpcInsecure(host, port);
        FlightClient client = FlightClient.builder(allocator, location).build();


        return new ApacheFlightConnection(client, allocator, location);
    }

    public void listFlightInfos() {
        Iterable<FlightInfo> flightInfosBefore = _client.listFlights(Criteria.ALL);
        System.out.println("===FLIGHTS INFO===");
        flightInfosBefore.forEach(t -> System.out.println(t));
    }

    //to delete, but could prove useful
    public static void insertTest(String filePath) {
        Path path = Paths.get(filePath);
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(
                        Files.readAllBytes(path)), rootAllocator)
        ) {
            while(reader.loadNextBatch()){
                System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close()  {
        try {
            AutoCloseables.close(_client, _allocator);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



