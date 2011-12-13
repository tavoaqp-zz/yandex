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

package org.apache.mahout.cf.taste.hadoop.slopeone;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;

public final class SlopeOneDiffsToAveragesReducer extends
    Reducer<EntityEntityWritable,FloatWritable, EntityEntityWritable,FloatWritable> {
  
  @Override
  protected void reduce(EntityEntityWritable key,
                        Iterable<FloatWritable> values,
                        Context context) throws IOException, InterruptedException {
    int count = 0;
    double total = 0.0;
    for (FloatWritable value : values) {
      total += value.get();
      count++;
    }
    context.write(key, new FloatWritable((float) (total / count)));
  }
}