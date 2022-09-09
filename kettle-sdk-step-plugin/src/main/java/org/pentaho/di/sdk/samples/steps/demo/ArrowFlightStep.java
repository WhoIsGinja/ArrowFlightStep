/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.sdk.samples.steps.demo;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.apache.arrow.flight.*;

import org.apache.arrow.vector.types.pojo.Field;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI.
 *
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *
 * This class is the implementation of StepInterface.
 * Classes implementing this interface need to:
 *
 * - initialize the step
 * - execute the row processing logic
 * - dispose of the step
 *
 * Please do not create any local fields in a StepInterface class. Store any
 * information related to the processing logic in the supplied step data interface
 * instead.
 *
 */

public class ArrowFlightStep extends BaseStep implements StepInterface {

  private static final Class<?> PKG = ArrowFlightStepMeta.class; // for i18n purposes

  /**
   * The constructor should simply pass on its arguments to the parent class.
   *
   * @param s                 step description
   * @param stepDataInterface step data class
   * @param c                 step copy
   * @param t                 transformation description
   * @param dis               transformation executing
   */
  public ArrowFlightStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
    super( s, stepDataInterface, c, t, dis );
  }

  /**
   * This method is called by PDI during transformation startup.
   *
   * It should initialize required for step execution.
   *
   * The meta and data implementations passed in can safely be cast
   * to the step's respective implementations.
   *
   * It is mandatory that super.init() is called to ensure correct behavior.
   *
   * Typical tasks executed here are establishing the connection to a database,
   * as wall as obtaining resources, like file handles.
   *
   * @param smi   step meta interface implementation, containing the step settings
   * @param sdi  step data interface implementation, used to store runtime information
   *
   * @return true if initialization completed successfully, false if there was an error preventing the step from working.
   *
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    try {
      // Casting to step-specific implementation classes is safe
      ArrowFlightStepMeta meta = (ArrowFlightStepMeta) smi;
      ArrowFlightStepData data = (ArrowFlightStepData) sdi;

      //TODO meter aqui tudo relacionado com iniciar conexao com a base de dados e cenas precisas para handling
      //TODO error handling
      if ( !super.init( meta, data ) ) {

        return false;
      }
      // Add any step-specific initialization that may be needed here

      //establishArrowConnection(meta, data);
      BufferAllocator allocator = new RootAllocator();
      //TODO deixar de ser hadcoded, para receber da GUI (alocados no meta)
      data.connection = ApacheFlightConnection.createFlightClient(allocator, "localhost", 8815);

      getData(meta, data);


      log.logBasic("Connected to server, client: " + data.connection.getClient());

      // Create row metadata with all the values in it...
      List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>(); // stores the errors...
      RowMetaAndData outputRow = buildRow( meta, data.fields, remarks, getStepname() );
      if ( !remarks.isEmpty() ) {
        //TODO mudar as mensagens de remarks, ou até dizer que um tipo está mal idk ou ate apagar isto tudo
        for ( int i = 0; i < remarks.size(); i++ ) {
          CheckResult cr = (CheckResult) remarks.get( i );
          logError( cr.getText() );
        }
        return false;
      }

      //TODO meter aqui uma funcao que passe o meta da row ou uma lista com os field names e types para usar no getfields e ver como se faz o append no rowgenerator

      data.outputRowMeta = outputRow.getRowMeta();
      log.logBasic(data.outputRowMeta.toString());


      return true;
    } catch ( Exception e ) {
      setErrors( 1L );
      logError( "Error initializing step", e );
      return false;
    }
  }

  public void getData(StepMetaInterface smi, StepDataInterface sdi) {
    ArrowFlightStepMeta meta = (ArrowFlightStepMeta) smi;
    ArrowFlightStepData data = (ArrowFlightStepData) sdi;


    //TODO passar a receber da GUI, meta onde estao alocados
    FlightInfo info = data.connection.getFlightInfo("more_profiles");

    data.stream = data.connection.getFlightStream(info.getDescriptor().getPath().get(0));

    data.fields = data.connection.getPDIFields(data.stream);
    data.input = data.connection.getFlightData(data.stream);

    //to make accountability for the first row being the fields names
    data.rowLimit = data.input.size() - 1;
    data.rowsWritten = 0L;

    log.logBasic("FLIGHT ROWS NO: " + data.rowLimit);
  }

  //TODO meter os nomes certos nos headers
  public static final RowMetaAndData buildRow(ArrowFlightStepMeta meta, List<Field> fields, List<CheckResultInterface> remarks,
                                              String origin ) throws KettlePluginException {
    RowMetaInterface rowMeta = new RowMeta();

    System.out.println("NUMBER OF COLS:  " + fields.size());
    Object[] rowData = new Object[fields.size()+ 2];

    String[] fieldnames = new String[fields.size()];

    for(int i = 0; i < fields.size(); i++) {
      fieldnames[i] = fields.get(i).getName();
    }

    meta.setFieldName(fieldnames);

    System.out.println(Arrays.toString(fieldnames));
    System.out.println(Arrays.toString(meta.getFieldName()));


    System.out.println("tamanho fields: " + fields.size());
    for ( int i = 0; i < fields.size(); i++ ) {

      int valtype = getValType(fields.get(i).getType().toString());
      System.out.println("FIELDTYPE: " + fields.get(i).getType().toString() + "  VALTYPE: " + valtype);

      if ( fields.get(i).getType() != null ) {
        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( meta.getFieldName()[i], valtype ); // build a value!


        rowMeta.addValueMeta( valueMeta );
      }
    }

    return new RowMetaAndData( rowMeta, rowData );
  }

  public static int getValType(String fieldTypeName) {

    String pdiDataType = null;

    switch (fieldTypeName) {
      case "Utf8" :
        pdiDataType = "String";

        break;

      case "Int(32, true)" :
        pdiDataType = "Integer";

        break;

      default:
        break;
    }


    int valtype = ValueMetaFactory.getIdForValueMeta(pdiDataType);
    System.out.println("type number: " + valtype);
    return valtype;
  }



  public void printInput( StepDataInterface sdi ) {
    ArrowFlightStepData data = (ArrowFlightStepData) sdi;

    for(Object[] row : data.input) {
      log.logBasic(Arrays.deepToString(row));
    }
  }

  public Object[] getRowInput( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    waitUntilTransformationIsStarted();

    ArrowFlightStepMeta meta = (ArrowFlightStepMeta) smi;
    ArrowFlightStepData data = (ArrowFlightStepData) sdi;

    Object[] row = null;

    data.inputLock.readLock().lock();
    try {
      if ( data.input.isEmpty() ) {
        return null;
      }

      row = data.input.get(data.rownr);
      data.rownr++;

    } finally {
      data.inputLock.readLock().unlock();
    }
    Object[] bufferRow = data.outputRowMeta.cloneRow( row );
    Object[] r = new Object[bufferRow.length];

    for(int i = 0; i < bufferRow.length; i++) {

      ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);

      ValueMetaInterface stringMeta =
              ValueMetaFactory.cloneValueMeta( valueMeta, ValueMetaInterface.TYPE_STRING );

      log.logBasic(" value meta " + valueMeta.toString());

      r[i] = valueMeta.convertData(stringMeta, bufferRow[i].toString());
    }

    return r;
  }

  /**
   * Once the transformation starts executing, the processRow() method is called repeatedly
   * by PDI for as long as it returns true. To indicate that a step has finished processing rows
   * this method must call setOutputDone() and return false;
   *
   * Steps which process incoming rows typically call getRow() to read a single row from the
   * input stream, change or add row content, call putRow() to pass the changed row on
   * and return true. If getRow() returns null, no more rows are expected to come in,
   * and the processRow() implementation calls setOutputDone() and returns false to
   * indicate that it is done too.
   *
   * Steps which generate rows typically construct a new row Object[] using a call to
   * RowDataUtil.allocateRowData(numberOfFields), add row content, and call putRow() to
   * pass the new row on. Above process may happen in a loop to generate multiple rows,
   * at the end of which processRow() would call setOutputDone() and return false;
   *
   * @param smi the step meta interface containing the step settings
   * @param sdi the step data interface that should be used to store
   *
   * @return true to indicate that the function should be called again, false if the step is done
   */
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    // safely cast the step settings (meta) and runtime info (data) to specific implementations
    ArrowFlightStepMeta meta = (ArrowFlightStepMeta) smi;
    ArrowFlightStepData data = (ArrowFlightStepData) sdi;

    Object[] r = null;
    Object[] bufferRow = null;

    if ( first ) {
      first = false;
    }

    if ( data.rowsWritten < data.rowLimit ) {
      bufferRow = getRowInput(smi, sdi);
      r = data.outputRowMeta.cloneRow( bufferRow );
    } else {
      setOutputDone();
      return false;
    }


    log.logBasic("=======SHOWING DATA==========");

    for(Object o: r) {
      log.logBasic("conteudo: " + o);
    }


    // log progress if it is time to to so
    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "DemoStep.Linenr", getLinesRead() ) ); // Some basic logging
    }

    log.logBasic(data.outputRowMeta.toString());
    log.logBasic(Arrays.deepToString(r));

    putRow( data.outputRowMeta, r );
    data.rowsWritten++;

    log.logBasic("rows written: " + data.rowsWritten);


    // indicate that processRow() should be called again
    return true;
  }

  /**
   * This method is called by PDI once the step is done processing.
   *
   * The dispose() method is the counterpart to init() and should release any resources
   * acquired for step execution like file handles or database connections.
   *
   * The meta and data implementations passed in can safely be cast
   * to the step's respective implementations.
   *
   * It is mandatory that super.dispose() is called to ensure correct behavior.
   *
   * @param smi   step meta interface implementation, containing the step settings
   * @param sdi  step data interface implementation, used to store runtime information
   */
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    // Casting to step-specific implementation classes is safe
    ArrowFlightStepMeta meta = (ArrowFlightStepMeta) smi;
    ArrowFlightStepData data = (ArrowFlightStepData) sdi;

    // Add any step-specific initialization that may be needed here
    log.logBasic("client:" + data.connection.getClient());
    data.connection.close();

    log.logBasic("antes dispose");
    // Call superclass dispose()
    super.dispose( meta, data );
    log.logBasic("depois dispose");
  }
}