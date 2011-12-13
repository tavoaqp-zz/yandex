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

package org.apache.mahout.cf.taste.impl.recommender.slopeone;

import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.TasteTestCase;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.cf.taste.model.DataModel;
import org.junit.Test;

/** Tests {@link MemoryDiffStorage}. */
public final class MemoryDiffStorageTest extends TasteTestCase {

  @Test
  public void testGetDiff() throws Exception {
    DataModel model = getDataModel();
    MemoryDiffStorage storage = new MemoryDiffStorage(model, Weighting.UNWEIGHTED, Long.MAX_VALUE);
    RunningAverage average = storage.getDiff(1, 2);
    assertEquals(0.23333333333333334, average.getAverage(), EPSILON);
    assertEquals(3, average.getCount());
  }

  @Test
  public void testAdd() throws Exception {
    DataModel model = getDataModel();
    MemoryDiffStorage storage = new MemoryDiffStorage(model, Weighting.UNWEIGHTED, Long.MAX_VALUE);

    RunningAverage average1 = storage.getDiff(0, 2);
    assertEquals(0.1, average1.getAverage(), EPSILON);
    assertEquals(3, average1.getCount());

    RunningAverage average2 = storage.getDiff(1, 2);
    assertEquals(0.23333332935969034, average2.getAverage(), EPSILON);
    assertEquals(3, average2.getCount());

    storage.addItemPref(1, 2, 0.8f);

    average1 = storage.getDiff(0, 2);
    assertEquals(0.25, average1.getAverage(), EPSILON);
    assertEquals(4, average1.getCount());

    average2 = storage.getDiff(1, 2);
    assertEquals(0.3, average2.getAverage(), EPSILON);
    assertEquals(4, average2.getCount());
  }

  @Test
  public void testUpdate() throws Exception {
    DataModel model = getDataModel();
    MemoryDiffStorage storage = new MemoryDiffStorage(model, Weighting.UNWEIGHTED, Long.MAX_VALUE);

    RunningAverage average = storage.getDiff(1, 2);
    assertEquals(0.23333332935969034, average.getAverage(), EPSILON);
    assertEquals(3, average.getCount());

    storage.updateItemPref(1, 0.5f);

    average = storage.getDiff(1, 2);
    assertEquals(0.06666666666666668, average.getAverage(), EPSILON);
    assertEquals(3, average.getCount());
  }

  @Test
  public void testRemove() throws Exception {
    DataModel model = getDataModel();
    MemoryDiffStorage storage = new MemoryDiffStorage(model, Weighting.UNWEIGHTED, Long.MAX_VALUE);

    RunningAverage average1 = storage.getDiff(0, 2);
    assertEquals(0.1, average1.getAverage(), EPSILON);
    assertEquals(3, average1.getCount());

    RunningAverage average2 = storage.getDiff(1, 2);
    assertEquals(0.23333332935969034, average2.getAverage(), EPSILON);
    assertEquals(3, average2.getCount());

    storage.removeItemPref(4, 2, 0.8f);

    average1 = storage.getDiff(0, 2);
    assertEquals(0.1, average1.getAverage(), EPSILON);
    assertEquals(2, average1.getCount());

    average2 = storage.getDiff(1, 2);
    assertEquals(0.1, average2.getAverage(), EPSILON);
    assertEquals(2, average2.getCount());
  }

}
