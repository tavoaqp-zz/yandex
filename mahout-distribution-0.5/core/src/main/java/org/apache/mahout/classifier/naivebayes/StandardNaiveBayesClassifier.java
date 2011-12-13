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
 * Class implementing the Naive Bayes Classifier Algorithm
 * 
 */
public class StandardNaiveBayesClassifier extends AbstractNaiveBayesClassifier { 
 
  public StandardNaiveBayesClassifier(NaiveBayesModel model) {
    super(model);
  }

  @Override
  public double getScoreForLabelFeature(int label, int feature) {
    NaiveBayesModel model = getModel();
    double result = model.getWeightMatrix().get(label, feature);
    double vocabCount = model.getVocabCount();
    double sumLabelWeight = model.getLabelSum().get(label);
    double numerator = result + model.getAlphaI();
    double denominator = sumLabelWeight + vocabCount;
    double weight = -Math.log(numerator / denominator);
    result = weight / model.getPerlabelThetaNormalizer().get(label);
    return result;
  }
  
}
