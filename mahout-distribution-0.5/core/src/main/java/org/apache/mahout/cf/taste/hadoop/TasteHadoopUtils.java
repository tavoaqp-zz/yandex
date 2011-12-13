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

package org.apache.mahout.cf.taste.hadoop;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

/**
 * Some helper methods for the hadoop-related stuff in org.apache.mahout.cf.taste
 */
public final class TasteHadoopUtils {

  /** Standard delimiter of textual preference data */
  private static final Pattern PREFERENCE_TOKEN_DELIMITER = Pattern.compile("[\t,]");

  private TasteHadoopUtils() {
  }

  /**
   * Splits a preference data line into string tokens
   */
  public static String[] splitPrefTokens(CharSequence line) {
    return PREFERENCE_TOKEN_DELIMITER.split(line);
  }

  /**
   * Maps a long to an int
   */
  public static int idToIndex(long id) {
    return 0x7FFFFFFF & Longs.hashCode(id);
  }

  /**
   * Reads a binary mapping file
   */
  public static OpenIntLongHashMap readItemIDIndexMap(String itemIDIndexPathStr, Configuration conf) {
    OpenIntLongHashMap indexItemIDMap = new OpenIntLongHashMap();
    Path itemIDIndexPath = new Path(itemIDIndexPathStr);
    for (Pair<VarIntWritable,VarLongWritable> record
         : new SequenceFileDirIterable<VarIntWritable,VarLongWritable>(itemIDIndexPath,
                                                                       PathType.LIST,
                                                                       PathFilters.partFilter(),
                                                                       null,
                                                                       true,
                                                                       conf)) {
      indexItemIDMap.put(record.getFirst().get(), record.getSecond().get());
    }
    return indexItemIDMap;
  }

  /**
   * Reads a text-based outputfile that only contains an int
   */
  public static int readIntFromFile(Configuration conf, Path outputDir) throws IOException {
    FileSystem fs = outputDir.getFileSystem(conf);
    Path outputFile = fs.listStatus(outputDir, PathFilters.partFilter())[0].getPath();
    InputStream in = null;
    try  {
      in = fs.open(outputFile);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, out, conf);
      return Integer.parseInt(new String(out.toByteArray(), Charsets.UTF_8).trim());
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
