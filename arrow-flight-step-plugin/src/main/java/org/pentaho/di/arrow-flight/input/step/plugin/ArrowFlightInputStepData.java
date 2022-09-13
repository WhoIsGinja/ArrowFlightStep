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

package org.pentaho.di.sdk.samples.steps.arrow;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.arrow.flight.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.List;


public class ArrowFlightInputStepData extends BaseStepData implements StepDataInterface {

  public ApacheFlightConnection connection;
  public FlightStream stream;

  public long rowLimit;
  public long rowsWritten;
  public ArrayList<Object[]> input;

  public int rownr = 1;

  public List<Field> fields;

  public RowMetaInterface outputRowMeta;

  public int outputFieldIndex = -1;

  public ReadWriteLock inputLock = new ReentrantReadWriteLock();

  public ArrowFlightInputStepData() {
    super();
  }

}
