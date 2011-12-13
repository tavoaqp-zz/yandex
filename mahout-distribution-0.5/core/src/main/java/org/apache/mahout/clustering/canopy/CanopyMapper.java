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

package org.apache.mahout.clustering.canopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

class CanopyMapper extends Mapper<WritableComparable<?>, VectorWritable, Text, VectorWritable> {

  private final Collection<Canopy> canopies = new ArrayList<Canopy>();

  private CanopyClusterer canopyClusterer;

  @Override
  protected void map(WritableComparable<?> key, VectorWritable point, Context context)
    throws IOException, InterruptedException {
    canopyClusterer.addPointToCanopies(point.get(), canopies);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    canopyClusterer = new CanopyClusterer(context.getConfiguration());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (Canopy canopy : canopies) {
      context.write(new Text("centroid"), new VectorWritable(canopy.computeCentroid()));
    }
    super.cleanup(context);
  }
}
