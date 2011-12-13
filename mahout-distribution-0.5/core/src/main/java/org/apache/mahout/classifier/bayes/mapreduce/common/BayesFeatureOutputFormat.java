/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.classifier.bayes.mapreduce.common;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.mahout.common.StringTuple;

/**
 * This class extends the MultipleOutputFormat, allowing to write the output data to different output files in
 * sequence file output format.
 */
public class BayesFeatureOutputFormat extends MultipleOutputFormat<WritableComparable<?>,Writable> {
  
  private SequenceFileOutputFormat<WritableComparable<?>,Writable> theSequenceFileOutputFormat;
  
  @Override
  protected RecordWriter<WritableComparable<?>,Writable> getBaseRecordWriter(FileSystem fs,
                                                                             JobConf job,
                                                                             String name,
                                                                             Progressable arg3) throws IOException {
    if (theSequenceFileOutputFormat == null) {
      theSequenceFileOutputFormat = new SequenceFileOutputFormat<WritableComparable<?>,Writable>();
    }
    return theSequenceFileOutputFormat.getRecordWriter(fs, job, name, arg3);
  }
  
  @Override
  protected String generateFileNameForKeyValue(WritableComparable<?> k, Writable v, String name) {
    StringTuple key = (StringTuple) k;
    if (key.length() == 3) {
      if (key.stringAt(0).equals(BayesConstants.WEIGHT)) {
        return "trainer-wordFreq/" + name;
      } else if (key.stringAt(0).equals(BayesConstants.DOCUMENT_FREQUENCY)) {
        return "trainer-termDocCount/" + name;
      }
    } else if (key.length() == 2) {
      if (key.stringAt(0).equals(BayesConstants.FEATURE_COUNT)) {
        return "trainer-featureCount/" + name;
      } else if (key.stringAt(0).equals(BayesConstants.LABEL_COUNT)) {
        return "trainer-docCount/" + name;
      }
    }
    throw new IllegalArgumentException("Unrecognized Tuple: " + key);
  }
  
}
