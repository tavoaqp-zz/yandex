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

package org.apache.mahout.df.mapreduce;

import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mahout.df.DecisionForest;
import org.apache.mahout.df.DFUtils;
import org.apache.mahout.df.data.DataConverter;
import org.apache.mahout.df.data.Dataset;
import org.apache.mahout.df.data.Instance;
import org.apache.mahout.common.RandomUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.net.URI;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;

/**
 * Mapreduce implementation that classifies the Input data using a previousely built decision forest
 */
public class Classifier {

  private static final Logger log = LoggerFactory.getLogger(Classifier.class);

  private final Path forestPath;
  private final Path inputPath;
  private final Path datasetPath;
  private final Configuration conf;
  /** If not null, the Job will build the confusionMatrix. */
  private final ResultAnalyzer analyzer;
  private final Dataset dataset;
  private final Path outputPath; // path that will containt the final output of the classifier
  private final Path mappersOutputPath; // mappers will output here

  public ResultAnalyzer getAnalyzer() {
    return analyzer;
  }

  public Classifier(Path forestPath,
                    Path inputPath,
                    Path datasetPath,
                    Path outputPath,
                    Configuration conf,
                    boolean analyze) throws IOException {
    this.forestPath = forestPath;
    this.inputPath = inputPath;
    this.datasetPath = datasetPath;
    this.outputPath = outputPath;
    this.conf = conf;

    if (analyze) {
      dataset = Dataset.load(conf, datasetPath);
      analyzer = new ResultAnalyzer(Arrays.asList(dataset.labels()), "unknown");

    } else {
      dataset = null;
      analyzer = null;
    }

    mappersOutputPath = new Path(outputPath, "mappers");
  }

  private void configureJob(Job job) throws IOException {

    job.setJarByClass(Classifier.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, mappersOutputPath);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(CMapper.class);
    job.setNumReduceTasks(0); // no reducers

    job.setInputFormatClass(CTextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

  }

  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    FileSystem fs = FileSystem.get(conf);

    // check the output
    if (fs.exists(outputPath)) {
      throw new IOException("Output path already exists : " + outputPath);
    }

    log.info("Adding the dataset to the DistributedCache");
    // put the dataset into the DistributedCache
    DistributedCache.addCacheFile(datasetPath.toUri(), conf);

    log.info("Adding the decision forest to the DistributedCache");
    DistributedCache.addCacheFile(forestPath.toUri(), conf);

    Job job = new Job(conf, "decision forest classifier");

    log.info("Configuring the job...");
    configureJob(job);

    log.info("Running the job...");
    if (!job.waitForCompletion(true)) {
      log.error("Job failed!");
      return;
    }

    parseOutput(job);

    HadoopUtil.delete(conf, mappersOutputPath);
  }

  /**
   * Extract the prediction for each mapper and write them in the corresponding output file. 
   * The name of the output file is based on the name of the corresponding input file.
   * Will compute the ConfusionMatrix if necessary.
   */
  private void parseOutput(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    FileSystem fs = mappersOutputPath.getFileSystem(conf);

    Path[] outfiles = DFUtils.listOutputFiles(fs, mappersOutputPath);

    // read all the output
    for (Path path : outfiles) {
      FSDataOutputStream ofile = null;
      try {
        for (Pair<LongWritable,Text> record : new SequenceFileIterable<LongWritable,Text>(path, true, conf)) {
          int key = (int) record.getFirst().get();
          String value = record.getSecond().toString();
          if (ofile == null) {
            // this is the first value, it contains the name of the input file
            ofile = fs.create(new Path(outputPath, value).suffix(".out"));
          } else {
            // The key contains the correct label of the data. The value contains a prediction
            ofile.writeChars(value); // write the prediction
            ofile.writeChar('\n');

            if (analyzer != null) {
              analyzer.addInstance(dataset.getLabel(key),
                                   new ClassifierResult(dataset.getLabel(Integer.parseInt(value)), 1.0));
            }
          }
        }
      } finally {
        ofile.close();
      }
    }

  }

  /**
   * TextInputFormat that does not split the input files. This ensures that each input file is processed by one single
   * mapper.
   */
  public static class CTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(JobContext jobContext, Path path) {
      return false;
    }
  }
  
  public static class CMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    /** used to convert input values to data instances */
    private DataConverter converter;
    private DecisionForest forest;
    private final Random rng = RandomUtils.getRandom();
    private boolean first = true;
    private final Text lvalue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.

      Configuration conf = context.getConfiguration();

      URI[] files = DistributedCache.getCacheFiles(conf);

      if (files == null || files.length < 2) {
        throw new IOException("not enough paths in the DistributedCache");
      }

      Dataset dataset = Dataset.load(conf, new Path(files[0].getPath()));

      converter = new DataConverter(dataset);

      forest = DecisionForest.load(conf, new Path(files[1].getPath()));
      if (forest == null) {
        throw new InterruptedException("DecisionForest not found!");
      }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      if (first) {
        FileSplit split = (FileSplit) context.getInputSplit();
        Path path = split.getPath(); // current split path
        lvalue.set(path.getName());
        context.write(key, lvalue);

        first = false;
      }

      String line = value.toString();
      if (!line.isEmpty()) {
        Instance instance = converter.convert(0, line);
        int prediction = forest.classify(rng, instance);
        key.set(instance.getLabel());
        lvalue.set(Integer.toString(prediction));
        context.write(key, lvalue);
      }
    }
  }
}
