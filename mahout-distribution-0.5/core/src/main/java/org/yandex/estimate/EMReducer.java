package org.yandex.estimate;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

public class EMReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>{

//	public static float[] newParams(float val[], float resp[], float params[]) {
//		float new_params[] = new float[5];
//		// Calculate mu1;
//		float num = 0;
//		float den = 0;
//		for (int i = 0; i < val.length; i++) {
//			num += (1 - resp[i]) * val[i];
//			den += (1 - resp[i]);
//		}
//		new_params[MU1] = num / den;
//		// Calculate mu2;
//		num = 0;
//		den = 0;
//		for (int i = 0; i < val.length; i++) {
//			num += resp[i] * val[i];
//			den += resp[i];
//		}
//		new_params[MU2] = num / den;
//		// Calculate sigma_sqr1;
//		num = 0;
//		den = 0;
//		for (int i = 0; i < val.length; i++) {
//			num += (1 - resp[i]) * Math.pow(val[i] - new_params[0], 2);
//			den += (1 - resp[i]);
//		}
//		new_params[SIGMA1] = num / den;
//		// Calculate sigma_sqr2;
//		num = 0;
//		den = 0;
//		for (int i = 0; i < val.length; i++) {
//			num += (resp[i]) * Math.pow(val[i] - new_params[1], 2);
//			den += resp[i];
//		}
//		new_params[SIGMA2] = num / den;
//
//		// Calculate pi_approx;
//		num = 0;
//		den = val.length;
//		for (int i = 0; i < val.length; i++) {
//			num += resp[i];
//		}
//		new_params[PI_APPROX] = num / den;
//		return new_params;
//	}
	
}
