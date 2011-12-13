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

package org.apache.mahout.classifier.naivebayes;

/**
 * Class containing Constants used by Naive Bayes classifier classes
 * 
 */
public final class BayesConstants {
  
  // Ensure all the strings are unique
  //public static final String ALPHA_SMOOTHING_FACTOR = "__SF"; // -
  
  //public static final String WEIGHT = "__WT";
  
  public static final String FEATURE_SUM = "__SJ";
  
  public static final String LABEL_SUM = "__SK";
  
  public static final String LABEL_THETA_NORMALIZER = "_LTN";
  
  private BayesConstants() { }
}
