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

package org.apache.mahout.df.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.mahout.df.data.Dataset.Attribute;

/**
 * Contains various methods that deal with descriptor strings
 */
public final class DescriptorUtils {
  private DescriptorUtils() { }
  
  /**
   * Parses a descriptor string and generates the corresponding array of Attributes
   * 
   * @throws DescriptorException
   *           if a bad token is encountered
   */
  public static Attribute[] parseDescriptor(String descriptor) throws DescriptorException {
    StringTokenizer tokenizer = new StringTokenizer(descriptor);
    Attribute[] attributes = new Attribute[tokenizer.countTokens()];
    
    for (int attr = 0; attr < attributes.length; attr++) {
      String token = tokenizer.nextToken().toUpperCase(Locale.ENGLISH);
      if ("I".equals(token)) {
        attributes[attr] = Attribute.IGNORED;
      } else if ("N".equals(token)) {
        attributes[attr] = Attribute.NUMERICAL;
      } else if ("C".equals(token)) {
        attributes[attr] = Attribute.CATEGORICAL;
      } else if ("L".equals(token)) {
        attributes[attr] = Attribute.LABEL;
      } else {
        throw new DescriptorException("Bad Token : " + token);
      }
    }
    
    return attributes;
  }
  
  /**
   * Generates a valid descriptor string from a user-friendly representation.<br>
   * for example "3 N I N N 2 C L 5 I" generates "N N N I N N C C L I I I I I".<br>
   * this useful when describing datasets with a large number of attributes
   */
  public static String generateDescriptor(String description) throws DescriptorException {
    StringTokenizer tokenizer = new StringTokenizer(description, " ");
    Collection<String> tokens = new ArrayList<String>();
    
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken());
    }
    
    return generateDescriptor(tokens);
  }
  
  /**
   * Generates a valid descriptor string from a list of tokens
   */
  public static String generateDescriptor(Iterable<String> tokens) throws DescriptorException {
    StringBuilder descriptor = new StringBuilder();
    
    int multiplicator = 0;
    
    for (String token : tokens) {
      try {
        // try to parse an integer
        int number = Integer.parseInt(token);
        
        if (number <= 0) {
          throw new DescriptorException("Multiplicator (" + number + ") must be > 0");
        }
        if (multiplicator > 0) {
          throw new DescriptorException("A multiplicator cannot be followed by another multiplicator");
        }
        
        multiplicator = number;
      } catch (NumberFormatException e) {
        // token is not a number
        if (multiplicator == 0) {
          multiplicator = 1;
        }
        
        for (int index = 0; index < multiplicator; index++) {
          descriptor.append(token).append(' ');
        }
        
        multiplicator = 0;
      }
    }
    
    return descriptor.toString().trim();
  }
}
