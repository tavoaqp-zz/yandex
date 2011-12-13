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
package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Implement this interface if you wish to extend SequenceFilesFromDirectory with your own parsing logic.
 */
public abstract class SequenceFilesFromDirectoryFilter extends SequenceFilesFromDirectory implements PathFilter {
  private static final Logger log = LoggerFactory.getLogger(SequenceFilesFromDirectoryFilter.class);

  protected final String prefix;
  protected final ChunkedWriter writer;
  protected final Charset charset;
  protected final Configuration conf;
  protected final FileSystem fs;
  protected final Map<String, String> options;

  protected SequenceFilesFromDirectoryFilter() {
    this.prefix = null;
    this.writer = null;
    this.charset = null;
    this.conf = null;
    this.fs = null;
    this.options = null;
  }

  protected SequenceFilesFromDirectoryFilter(Configuration conf,
                                             String keyPrefix,
                                             Map<String, String> options,
                                             ChunkedWriter writer)
    throws IOException {
    this.conf = conf;
    this.prefix = keyPrefix;
    this.writer = writer;
    this.charset = Charset.forName(options.get(SequenceFilesFromDirectory.CHARSET_OPTION[0]));
    this.fs = FileSystem.get(conf);
    this.options = options;
  }

  protected final Map<String, String> getOptions() {
    return options;
  }

  @Override
  public final boolean accept(Path current) {
    log.debug("CURRENT: {}", current.getName());
    try {
      for (FileStatus fst : fs.listStatus(current)) {
        log.debug("CHILD: {}", fst.getPath().getName());
        process(fst, current);
      }
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    return false;
  }

  protected abstract void process(FileStatus in, Path current) throws IOException;
}
