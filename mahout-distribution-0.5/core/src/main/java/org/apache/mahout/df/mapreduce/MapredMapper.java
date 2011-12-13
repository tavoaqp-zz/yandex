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

package org.apache.mahout.df.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.df.builder.TreeBuilder;
import org.apache.mahout.df.data.Dataset;

import com.google.common.base.Preconditions;

/**
 * Base class for Mapred mappers. Loads common parameters from the job
 */
public class MapredMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  
  private boolean noOutput;
  
  private boolean oobEstimate;
  
  private TreeBuilder treeBuilder;
  
  private Dataset dataset;
  
  /**
   * 
   * @return if false, the mapper does not output
   */
  protected boolean isOobEstimate() {
    return oobEstimate;
  }
  
  /**
   * 
   * @return if false, the mapper does not estimate and output predictions
   */
  protected boolean isNoOutput() {
    return noOutput;
  }
  
  protected TreeBuilder getTreeBuilder() {
    return treeBuilder;
  }
  
  protected Dataset getDataset() {
    return dataset;
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    
    Configuration conf = context.getConfiguration();
    
    configure(!Builder.isOutput(conf), Builder.isOobEstimate(conf), Builder.getTreeBuilder(conf), Builder
        .loadDataset(conf));
  }
  
  /**
   * Useful for testing
   */
  protected void configure(boolean noOutput, boolean oobEstimate, TreeBuilder treeBuilder, Dataset dataset) {
    Preconditions.checkArgument(treeBuilder != null, "TreeBuilder not found in the Job parameters");
    this.noOutput = noOutput;
    this.oobEstimate = oobEstimate;
    this.treeBuilder = treeBuilder;
    this.dataset = dataset;
  }
}
