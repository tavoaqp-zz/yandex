package org.yandex.estimate;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

public class EMMapper extends TableMapper<ImmutableBytesWritable, IntWritable>{

	public static float pdf(float val, float mu, float sigma_sqr) {
		float numerator = (float) Math.exp((-Math.pow(val - mu, 2))
				/ (2 * sigma_sqr));
		float denominator = (float) Math.sqrt(2 * Math.PI * sigma_sqr);
		return numerator / denominator;
	}
	
//	public static float resp(float val, float params[]) {
//		float numerator = params[PI_APPROX] * pdf(val, params[MU2], params[SIGMA2]);
//		float denominator = (1 - params[PI_APPROX]) * pdf(val, params[MU1], params[SIGMA1]) + params[PI_APPROX]
//				* pdf(val, params[MU2], params[SIGMA2]);
//		return numerator / denominator;
//	}
}
