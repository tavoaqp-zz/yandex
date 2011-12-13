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

package org.apache.mahout.fpm.pfpgrowth;

import java.io.File;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PFPGrowthTest extends MahoutTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(PFPGrowthTest.class);
  
  private final Parameters params = new Parameters();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    params.set(PFPGrowth.MIN_SUPPORT, "3");
    params.set(PFPGrowth.MAX_HEAPSIZE, "4");
    params.set(PFPGrowth.NUM_GROUPS, "2");
    params.set(PFPGrowth.ENCODING, "UTF-8");
    File inputDir = getTestTempDir("transactions");
    File outputDir = getTestTempDir("frequentpatterns");
    File input = new File(inputDir, "test.txt");
    params.set(PFPGrowth.INPUT, input.getAbsolutePath());
    params.set(PFPGrowth.OUTPUT, outputDir.getAbsolutePath());
    Writer writer = Files.newWriter(input, Charsets.UTF_8);
    try {
      Collection<List<String>> transactions = new ArrayList<List<String>>();
      transactions.add(Arrays.asList("E", "A", "D", "B"));
      transactions.add(Arrays.asList("D", "A", "C", "E", "B"));
      transactions.add(Arrays.asList("C", "A", "B", "E"));
      transactions.add(Arrays.asList("B", "A", "D"));
      transactions.add(Arrays.asList("D"));
      transactions.add(Arrays.asList("D", "B"));
      transactions.add(Arrays.asList("A", "D", "E"));
      transactions.add(Arrays.asList("B", "C"));
      for (List<String> transaction : transactions) {
        String sep = "";
        for (String item : transaction) {
          writer.write(sep + item);
          sep = ",";
        }
        writer.write("\n");
      }
    } finally {
      writer.close();
    }
    
  }

  @Test
  public void testStartParallelFPGrowth() throws Exception {
    log.info("Starting Parallel Counting Test: {}", params.get(PFPGrowth.MAX_HEAPSIZE));
    PFPGrowth.startParallelCounting(params);
    log.info("Reading fList Test: {}", params.get(PFPGrowth.MAX_HEAPSIZE));
    List<Pair<String,Long>> fList = PFPGrowth.readFList(params);
    log.info("{}", fList);
    assertEquals("[(B,6), (D,6), (A,5), (E,4), (C,3)]", fList.toString());
 
    log.info("Starting Grouping Test: {}", params.get(PFPGrowth.MAX_HEAPSIZE));
    PFPGrowth.startGroupingItems(params);
    Map<String,Long> gList = PFPGrowth.deserializeMap(params, PFPGrowth.G_LIST, new Configuration());
    log.info("{}", gList);
    assertEquals("{D=0, E=1, A=0, B=0, C=1}", gList.toString());
 
    log.info("Starting Parallel FPGrowth Test: {}", params.get(PFPGrowth.MAX_HEAPSIZE));
    PFPGrowth.startGroupingItems(params);
    PFPGrowth.startTransactionSorting(params);
    PFPGrowth.startParallelFPGrowth(params);
    log.info("Starting Pattern Aggregation Test: {}", params.get(PFPGrowth.MAX_HEAPSIZE));
    PFPGrowth.startAggregating(params);
    List<Pair<String,TopKStringPatterns>> frequentPatterns = PFPGrowth.readFrequentPattern(params);
    assertEquals("[(A,([A],5), ([D, A],4), ([B, A],4), ([A, E],4)), "
                 + "(B,([B],6), ([B, D],4), ([B, A],4), ([B, D, A],3)), " 
                 + "(C,([B, C],3)), "
                 + "(D,([D],6), ([D, A],4), ([B, D],4), ([D, A, E],3)), "
                 + "(E,([A, E],4), ([D, A, E],3), ([B, A, E],3))]", frequentPatterns.toString());
    
  }
}
