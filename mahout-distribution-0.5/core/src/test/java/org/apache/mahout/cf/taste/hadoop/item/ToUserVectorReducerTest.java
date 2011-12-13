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

package org.apache.mahout.cf.taste.hadoop.item;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.TasteTestCase;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.MathHelper;
import org.easymock.classextension.EasyMock;
import org.junit.Test;

import java.util.Arrays;

/**
 * tests {@link org.apache.mahout.cf.taste.hadoop.item.ToUserVectorReducer}
 */
public class ToUserVectorReducerTest extends TasteTestCase {

  @Test
  public void testToUsersReducerMinPreferencesUserIgnored() throws Exception {
    Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable>.Context context =
        EasyMock.createMock(Reducer.Context.class);

    ToUserVectorReducer reducer = new ToUserVectorReducer();
    setField(reducer, "minPreferences", 2);

    EasyMock.replay(context);

    reducer.reduce(new VarLongWritable(123), Arrays.asList(new VarLongWritable(456)), context);

    EasyMock.verify(context);
  }

  @Test
  public void testToUsersReducerMinPreferencesUserPasses() throws Exception {
    Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable>.Context context =
        EasyMock.createMock(Reducer.Context.class);

    ToUserVectorReducer reducer = new ToUserVectorReducer();
    setField(reducer, "minPreferences", 2);

    context.write(EasyMock.eq(new VarLongWritable(123)), MathHelper.vectorMatches(
        MathHelper.elem(TasteHadoopUtils.idToIndex(456L), 1.0), MathHelper.elem(TasteHadoopUtils.idToIndex(789L), 1.0)));

    EasyMock.replay(context);

    reducer.reduce(new VarLongWritable(123), Arrays.asList(new VarLongWritable(456), new VarLongWritable(789)), context);

    EasyMock.verify(context);
  }

}
