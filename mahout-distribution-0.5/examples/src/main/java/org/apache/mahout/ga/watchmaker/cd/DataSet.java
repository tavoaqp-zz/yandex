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

package org.apache.mahout.ga.watchmaker.cd;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Contains information about the dataset and its attributes. The label is a
 * nominal attribute with a known position. Ignored attributes are not taken
 * into account when calculating attribute's position.
 */
public class DataSet {
  
  /** Singleton */
  private static DataSet dataset;
  
  private final List<Integer> ignoredAttributes;
  
  private final int labelIndex;
  
  private final List<Attribute> attributes;
  
  // This constructor just exists to satisfy xstream
  
  DataSet() {
    this(null, null, 0);
  }
  
  DataSet(List<Attribute> attributes, List<Integer> ignored, int labelIndex) {
    this.attributes = attributes;
    ignoredAttributes = ignored;
    this.labelIndex = labelIndex;
  }
  
  /**
   * Singleton DataSet
   * 
   * @throws IllegalStateException if the dataset has not been initialized
   */
  public static DataSet getDataSet() {
    if (dataset == null) {
      throw new IllegalStateException("DataSet not initialized");
    }
    return dataset;
  }
  
  /**
   * Initializes the singleton dataset
   * 
   * @param dataset
   */
  public static void initialize(DataSet dataset) {
    DataSet.dataset = dataset;
  }
  
  /**
   * @return number of non-ignored attributes.
   */
  public int getNbAttributes() {
    return attributes.size();
  }
  
  /**
   * @return Indexes of the ignored attributes, if any.
   */
  public List<Integer> getIgnoredAttributes() {
    return ignoredAttributes;
  }
  
  /**
   * @return zero-based position of the label in the dataset.
   */
  public int getLabelIndex() {
    return labelIndex;
  }
  
  /**
   * Maximum possible value for an attribute
   * 
   * @param index of the attribute
   * @throws IllegalArgumentException if the attribute is nominal
   */
  public double getMax(int index) {
    Preconditions.checkArgument(isNumerical(index), "Nominal attribute");
    return ((NumericalAttr) attributes.get(index)).getMax();
  }
  
  /**
   * Minimum possible value for an attribute
   * 
   * @param index of the attribute
   * @throws IllegalArgumentException if the attribute is nominal
   */
  public double getMin(int index) {
    Preconditions.checkArgument(isNumerical(index), "Nominal attribute");
    return ((NumericalAttr) attributes.get(index)).getMin();
  }
  
  /**
   * Number of values for a nominal attribute
   * 
   * @param index of the attribute
   * @throws IllegalArgumentException if the attribute is numerical
   */
  public int getNbValues(int index) {
    Preconditions.checkArgument(!isNumerical(index), "Numerical attribute");
    return ((NominalAttr) attributes.get(index)).getNbvalues();
  }
  
  /**
   * Is the attribute numerical or nominal ?
   * 
   * @param index of the attribute
   * @return true is numerical, false if nominal
   */
  public boolean isNumerical(int index) {
    return attributes.get(index).isNumerical();
  }
  
  /**
   * Converts a string value of a nominal attribute to an {@code int}.
   *
   * @param index of the attribute
   * @param value
   * @return an {@code int} representing the value
   * @throws IllegalArgumentException if the value is not found.
   */
  public int valueIndex(int index, String value) {
    Preconditions.checkArgument(!isNumerical(index), "Numerical attribute");
    return ((NominalAttr) attributes.get(index)).valueIndex(value);
  }
}
