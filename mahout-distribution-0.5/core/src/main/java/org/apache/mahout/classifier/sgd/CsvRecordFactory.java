/*
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

package org.apache.mahout.classifier.sgd;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.TextValueEncoder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts csv data lines to vectors.
 *
 * Use of this class proceeds in a few steps.
 * <ul>
 * <li> At construction time, you tell the class about the target variable and provide
 * a dictionary of the types of the predictor values.  At this point,
 * the class yet cannot decode inputs because it doesn't know the fields that are in the
 * data records, nor their order.
 * <li> Optionally, you tell the parser object about the possible values of the target
 * variable.  If you don't do this then you probably should set the number of distinct
 * values so that the target variable values will be taken from a restricted range.
 * <li> Later, when you get a list of the fields, typically from the first line of a CSV
 * file, you tell the factory about these fields and it builds internal data structures
 * that allow it to decode inputs.  The most important internal state is the field numbers
 * for various fields.  After this point, you can use the factory for decoding data.
 * <li> To encode data as a vector, you present a line of input to the factory and it
 * mutates a vector that you provide.  The factory also retains trace information so
 * that it can approximately reverse engineer vectors later.
 * <li> After converting data, you can ask for an explanation of the data in terms of
 * terms and weights.  In order to explain a vector accurately, the factory needs to
 * have seen the particular values of categorical fields (typically during encoding vectors)
 * and needs to have a reasonably small number of collisions in the vector encoding.
 * </ul>
 */
public class CsvRecordFactory implements RecordFactory {
  private static final String INTERCEPT_TERM = "Intercept Term";

  // crude CSV value splitter.  This will fail if any double quoted strings have
  // commas inside.  Also, escaped quotes will not be unescaped.  Good enough for now.
  private final Splitter onComma = Splitter.on(",").trimResults(CharMatcher.is('"'));

  private static final Map<String, Class<? extends FeatureVectorEncoder>> TYPE_DICTIONARY =
          ImmutableMap.<String, Class<? extends FeatureVectorEncoder>>builder()
                  .put("continuous", ContinuousValueEncoder.class)
                  .put("numeric", ContinuousValueEncoder.class)
                  .put("n", ContinuousValueEncoder.class)
                  .put("word", StaticWordValueEncoder.class)
                  .put("w", StaticWordValueEncoder.class)
                  .put("text", TextValueEncoder.class)
                  .put("t", TextValueEncoder.class)
                  .build();

  private final Map<String, Set<Integer>> traceDictionary = Maps.newTreeMap();

  private int target;
  private final Dictionary targetDictionary;

  private List<Integer> predictors;
  private Map<Integer, FeatureVectorEncoder> predictorEncoders;
  private int maxTargetValue = Integer.MAX_VALUE;
  private final String targetName;
  private final Map<String, String> typeMap;
  private List<String> variableNames;
  private boolean includeBiasTerm;
  private static final String CANNOT_CONSTRUCT_CONVERTER =
      "Unable to construct type converter... shouldn't be possible";

  /**
   * Construct a parser for CSV lines that encodes the parsed data in vector form.
   * @param targetName            The name of the target variable.
   * @param typeMap               A map describing the types of the predictor variables.
   */
  public CsvRecordFactory(String targetName, Map<String, String> typeMap) {
    this.targetName = targetName;
    this.typeMap = typeMap;
    targetDictionary = new Dictionary();
  }

  /**
   * Defines the values and thus the encoding of values of the target variables.  Note
   * that any values of the target variable not present in this list will be given the
   * value of the last member of the list.
   * @param values  The values the target variable can have.
   */
  @Override
  public void defineTargetCategories(List<String> values) {
    Preconditions.checkArgument(
        values.size() <= maxTargetValue,
        "Must have less than or equal to " + maxTargetValue + " categories for target variable, but found "
            + values.size());
    if (maxTargetValue == Integer.MAX_VALUE) {
      maxTargetValue = values.size();
    }

    for (String value : values) {
      targetDictionary.intern(value);
    }
  }

  /**
   * Defines the number of target variable categories, but allows this parser to
   * pick encodings for them as they appear.
   * @param max  The number of categories that will be excpeted.  Once this many have been
   * seen, all others will get the encoding max-1.
   */
  @Override
  public CsvRecordFactory maxTargetValue(int max) {
    maxTargetValue = max;
    return this;
  }

  @Override
  public boolean usesFirstLineAsSchema() {
    return true;
  }

  /**
   * Processes the first line of a file (which should contain the variable names). The target and
   * predictor column numbers are set from the names on this line.
   *
   * @param line       Header line for the file.
   */
  @Override
  public void firstLine(String line) {
    // read variable names, build map of name -> column
    final Map<String, Integer> vars = Maps.newHashMap();
    variableNames = Lists.newArrayList(onComma.split(line));
    int column = 0;
    for (String var : variableNames) {
      vars.put(var, column++);
    }

    // record target column and establish dictionary for decoding target
    target = vars.get(targetName);

    // create list of predictor column numbers
    predictors = Lists.newArrayList(Collections2.transform(typeMap.keySet(), new Function<String, Integer>() {
      @Override
      public Integer apply(String from) {
        Integer r = vars.get(from);
        Preconditions.checkArgument(r != null, "Can't find variable %s, only know about %s", from, vars);
        return r;
      }
    }));

    if (includeBiasTerm) {
      predictors.add(-1);
    }
    Collections.sort(predictors);

    // and map from column number to type encoder for each column that is a predictor
    predictorEncoders = Maps.newHashMap();
    for (Integer predictor : predictors) {
      String name;
      Class<? extends FeatureVectorEncoder> c;
      if (predictor == -1) {
        name = INTERCEPT_TERM;
        c = ConstantValueEncoder.class;
      } else {
        name = variableNames.get(predictor);
        c = TYPE_DICTIONARY.get(typeMap.get(name));
      }
      try {
        Preconditions.checkArgument(c != null, "Invalid type of variable %s,  wanted one of %s",
          typeMap.get(name), TYPE_DICTIONARY.keySet());
        Constructor<? extends FeatureVectorEncoder> constructor = c.getConstructor(String.class);
        Preconditions.checkArgument(constructor != null, "Can't find correct constructor for %s", typeMap.get(name));
        FeatureVectorEncoder encoder = constructor.newInstance(name);
        predictorEncoders.put(predictor, encoder);
        encoder.setTraceDictionary(traceDictionary);
      } catch (InstantiationException e) {
        throw new ImpossibleException(CANNOT_CONSTRUCT_CONVERTER, e);
      } catch (IllegalAccessException e) {
        throw new ImpossibleException(CANNOT_CONSTRUCT_CONVERTER, e);
      } catch (InvocationTargetException e) {
        throw new ImpossibleException(CANNOT_CONSTRUCT_CONVERTER, e);
      } catch (NoSuchMethodException e) {
        throw new ImpossibleException(CANNOT_CONSTRUCT_CONVERTER, e);
      }
    }
  }


  /**
   * Decodes a single line of csv data and records the target and predictor variables in a record.
   * As a side effect, features are added into the featureVector.  Returns the value of the target
   * variable.
   *
   * @param line          The raw data.
   * @param featureVector Where to fill in the features.  Should be zeroed before calling
   *                      processLine.
   * @return The value of the target variable.
   */
  @Override
  public int processLine(String line, Vector featureVector) {
    List<String> values = Lists.newArrayList(onComma.split(line));

    int targetValue = targetDictionary.intern(values.get(target));
    if (targetValue >= maxTargetValue) {
      targetValue = maxTargetValue - 1;
    }

    for (Integer predictor : predictors) {
      String value;
      if (predictor >= 0) {
        value = values.get(predictor);
      } else {
        value = null;
      }
      predictorEncoders.get(predictor).addToVector(value, featureVector);
    }
    return targetValue;
  }

  /**
   * Returns a list of the names of the predictor variables.
   *
   * @return A list of variable names.
   */
  @Override
  public Iterable<String> getPredictors() {
    return Lists.transform(predictors, new Function<Integer, String>() {
      @Override
      public String apply(Integer v) {
        if (v >= 0) {
          return variableNames.get(v);
        } else {
          return INTERCEPT_TERM;
        }
      }
    });
  }

  @Override
  public Map<String, Set<Integer>> getTraceDictionary() {
    return traceDictionary;
  }

  @Override
  public CsvRecordFactory includeBiasTerm(boolean useBias) {
    includeBiasTerm = useBias;
    return this;
  }

  @Override
  public List<String> getTargetCategories() {
    List<String> r = targetDictionary.values();
    if (r.size() > maxTargetValue) {
      r.subList(maxTargetValue, r.size()).clear();
    }
    return r;
  }

  private static final class ImpossibleException extends RuntimeException {
    private ImpossibleException(String message, Throwable cause) {
      super(message, cause);
    }
  }

}
