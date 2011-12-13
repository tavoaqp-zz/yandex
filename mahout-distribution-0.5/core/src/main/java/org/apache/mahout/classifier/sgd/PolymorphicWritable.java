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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Utilities that write a class name and then serialize using writables.
 */
public final class PolymorphicWritable<T> {

  private PolymorphicWritable() {
  }

  public static <T extends Writable> void write(DataOutput dataOutput, T value) throws IOException {
    dataOutput.writeUTF(value.getClass().getName());
    value.write(dataOutput);
  }

  public static <T extends Writable> T read(DataInput dataInput, Class<? extends T> clazz) throws IOException {
    String className = dataInput.readUTF();
    T r = null;
    try {
      r = Class.forName(className).asSubclass(clazz).newInstance();
    } catch (InstantiationException e) {
      throw new IOException("Can't create object", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't access constructor", e);
    } catch (ClassNotFoundException e) {
      throw new IOException("No such class", e);
    }
    r.readFields(dataInput);
    return r;
  }
}
