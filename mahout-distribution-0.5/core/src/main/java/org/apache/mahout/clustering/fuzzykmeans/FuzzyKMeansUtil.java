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

package org.apache.mahout.clustering.fuzzykmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterable;

final class FuzzyKMeansUtil {

  private FuzzyKMeansUtil() {
  }

  /** Configure the mapper with the cluster info */
  public static void configureWithClusterInfo(Path clusterPathStr, Collection<SoftCluster> clusters)
    throws IOException {
    // Get the path location where the cluster Info is stored
    Configuration conf = new Configuration();
    Path clusterPath = new Path(clusterPathStr, "*");
    Collection<Path> result = new ArrayList<Path>();

    // get all filtered file names in result list
    FileSystem fs = clusterPath.getFileSystem(conf);
    FileStatus[] matches = fs.listStatus(FileUtil.stat2Paths(fs.globStatus(clusterPath, PathFilters.partFilter())),
                                         PathFilters.partFilter());

    for (FileStatus match : matches) {
      result.add(fs.makeQualified(match.getPath()));
    }

    // iterate through the result path list
    for (Path path : result) {
      for (Writable value : new SequenceFileValueIterable<Writable>(path, conf)) {
        Class<? extends Writable> valueClass = value.getClass();
        if (valueClass.equals(Cluster.class)) {
          // get the cluster info
          Cluster cluster = (Cluster) value;
          clusters.add(new SoftCluster(cluster.getCenter(), cluster.getId(), cluster.getMeasure()));
        } else if (valueClass.equals(SoftCluster.class)) {
          // get the cluster info
          clusters.add((SoftCluster) value);
        } else if (valueClass.equals(Canopy.class)) {
          // get the cluster info
          Canopy canopy = (Canopy) value;
          clusters.add(new SoftCluster(canopy.getCenter(), canopy.getId(), canopy.getMeasure()));
        } else {
          throw new IllegalStateException("Bad value class: " + valueClass);
        }
      }
    }

  }

}
