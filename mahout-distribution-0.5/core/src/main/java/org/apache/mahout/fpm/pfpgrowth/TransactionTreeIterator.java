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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import com.google.common.collect.AbstractIterator;
import org.apache.mahout.common.Pair;

/**
 * Generates a List of transactions view of Transaction Tree by doing Depth First Traversal on the tree
 * structure
 */
final class TransactionTreeIterator extends AbstractIterator<Pair<List<Integer>,Long>> {

  private final Stack<int[]> depth = new Stack<int[]>();
  private final TransactionTree transactionTree;

  TransactionTreeIterator(TransactionTree transactionTree) {
    this.transactionTree = transactionTree;
    depth.push(new int[] {0, -1});
  }

  @Override
  protected Pair<List<Integer>, Long> computeNext() {

    if (depth.isEmpty()) {
      return endOfData();
    }
    
    long sum;
    int childId;
    do {
      int[] top = depth.peek();
      while (top[1] + 1 == transactionTree.childCount(top[0])) {
        depth.pop();
        top = depth.peek();
      }
      if (depth.isEmpty()) {
        return endOfData();
      }
      top[1]++;
      childId = transactionTree.childAtIndex(top[0], top[1]);
      depth.push(new int[] {childId, -1});
      
      sum = 0;
      for (int i = transactionTree.childCount(childId) - 1; i >= 0; i--) {
        sum += transactionTree.count(transactionTree.childAtIndex(childId, i));
      }
    } while (sum == transactionTree.count(childId));

    List<Integer> data = new ArrayList<Integer>();
    Iterator<int[]> it = depth.iterator();
    it.next();
    while (it.hasNext()) {
      data.add(transactionTree.attribute(it.next()[0]));
    }

    Pair<List<Integer>,Long> returnable = new Pair<List<Integer>,Long>(data, transactionTree.count(childId) - sum);

    int[] top = depth.peek();
    while (top[1] + 1 == transactionTree.childCount(top[0])) {
      depth.pop();
      if (depth.isEmpty()) {
        break;
      }
      top = depth.peek();
    }
    return returnable;
  }


}
