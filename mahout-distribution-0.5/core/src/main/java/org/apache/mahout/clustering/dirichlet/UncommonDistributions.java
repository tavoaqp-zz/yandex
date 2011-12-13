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

package org.apache.mahout.clustering.dirichlet;

import java.util.Random;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.uncommons.maths.random.GaussianGenerator;

public final class UncommonDistributions {
  
  private static final double SQRT2PI = Math.sqrt(2.0 * Math.PI);
  
  private static final Random RANDOM = RandomUtils.getRandom();
  
  private UncommonDistributions() {
  }
  
  // =============== start of BSD licensed code. See LICENSE.txt
  /**
   * Returns a double sampled according to this distribution. Uniformly fast for all k > 0. (Reference:
   * Non-Uniform Random Variate Generation, Devroye http://cgm.cs.mcgill.ca/~luc/rnbookindex.html) Uses
   * Cheng's rejection algorithm (GB) for k>=1, rejection from Weibull distribution for 0 < k < 1.
   */
  public static double rGamma(double k, double lambda) {
    boolean accept = false;
    if (k >= 1.0) {
      // Cheng's algorithm
      double b = k - Math.log(4.0);
      double c = k + Math.sqrt(2.0 * k - 1.0);
      double lam = Math.sqrt(2.0 * k - 1.0);
      double cheng = 1.0 + Math.log(4.5);
      double x;
      do {
        double u = UncommonDistributions.RANDOM.nextDouble();
        double v = UncommonDistributions.RANDOM.nextDouble();
        double y = 1.0 / lam * Math.log(v / (1.0 - v));
        x = k * Math.exp(y);
        double z = u * v * v;
        double r = b + c * y - x;
        if (r >= 4.5 * z - cheng || r >= Math.log(z)) {
          accept = true;
        }
      } while (!accept);
      return x / lambda;
    } else {
      // Weibull algorithm
      double c = 1.0 / k;
      double d = (1.0 - k) * Math.pow(k, k / (1.0 - k));
      double x;
      do {
        double u = UncommonDistributions.RANDOM.nextDouble();
        double v = UncommonDistributions.RANDOM.nextDouble();
        double z = -Math.log(u);
        double e = -Math.log(v);
        x = Math.pow(z, c);
        if (z + e >= d + x) {
          accept = true;
        }
      } while (!accept);
      return x / lambda;
    }
  }
  
  // ============= end of BSD licensed code
  
  /**
   * Returns a random sample from a beta distribution with the given shapes
   * 
   * @param shape1
   *          a double representing shape1
   * @param shape2
   *          a double representing shape2
   * @return a Vector of samples
   */
  public static double rBeta(double shape1, double shape2) {
    double gam1 = rGamma(shape1, 1.0);
    double gam2 = rGamma(shape2, 1.0);
    return gam1 / (gam1 + gam2);
    
  }
  
  /**
   * Returns a vector of random samples from a beta distribution with the given shapes
   * 
   * @param k
   *          the number of samples to return
   * @param shape1
   *          a double representing shape1
   * @param shape2
   *          a double representing shape2
   * @return a Vector of samples
   */
  public static Vector rBeta(int k, double shape1, double shape2) {
    // List<Double> params = new ArrayList<Double>(2);
    // params.add(shape1);
    // params.add(Math.max(0, shape2));
    Vector result = new DenseVector(k);
    for (int i = 0; i < k; i++) {
      result.set(i, rBeta(shape1, shape2));
    }
    return result;
  }
  
  /**
   * Return a random sample from the chi-squared (chi^2) distribution with df degrees of freedom.
   * 
   * @return a double sample
   */
  public static double rChisq(double df) {
    double result = 0.0;
    for (int i = 0; i < df; i++) {
      double sample = rNorm(0.0, 1.0);
      result += sample * sample;
    }
    return result;
  }
  
  /**
   * Return a random value from a normal distribution with the given mean and standard deviation
   * 
   * @param mean
   *          a double mean value
   * @param sd
   *          a double standard deviation
   * @return a double sample
   */
  public static double rNorm(double mean, double sd) {
    GaussianGenerator dist = new GaussianGenerator(mean, sd, RANDOM);
    return dist.nextValue();
  }
  
  /**
   * Return the normal density function value for the sample x
   * 
   * pdf = 1/[sqrt(2*p)*s] * e^{-1/2*[(x-m)/s]^2}
   * 
   * @param x
   *          a double sample value
   * @param m
   *          a double mean value
   * @param s
   *          a double standard deviation
   * @return a double probability value
   */
  public static double dNorm(double x, double m, double s) {
    double xms = (x - m) / s;
    double ex = xms * xms / 2.0;
    double exp = Math.exp(-ex);
    return exp / (SQRT2PI * s);
  }
  
  /** Returns one sample from a multinomial. */
  public static int rMultinom(Vector probabilities) {
    // our probability argument are not normalized.
    double total = probabilities.zSum();
    double nextDouble = UncommonDistributions.RANDOM.nextDouble();
    double p = nextDouble * total;
    for (int i = 0; i < probabilities.size(); i++) {
      double pi = probabilities.get(i);
      if (p < pi) {
        return i;
      } else {
        p -= pi;
      }
    }
    // can't happen except for round-off error so we don't care what we return here
    return 0;
  }
  
  /**
   * Returns a multinomial vector sampled from the given probabilities
   * 
   * rmultinom should be implemented as successive binomial sampling.
   * 
   * Keep a normalizing amount that starts with 1 (I call it total).
   * 
   * For each i k[i] = rbinom(p[i] / total, size); total -= p[i]; size -= k[i];
   * 
   * @param size
   *          the size parameter of the binomial distribution
   * @param probabilities
   *          a Vector of probabilities
   * @return a multinomial distribution Vector
   */
  public static Vector rMultinom(int size, Vector probabilities) {
    // our probability argument may not be normalized.
    double total = probabilities.zSum();
    int cardinality = probabilities.size();
    Vector result = new DenseVector(cardinality);
    for (int i = 0; total > 0 && i < cardinality; i++) {
      double p = probabilities.get(i);
      int ki = rBinomial(size, p / total);
      total -= p;
      size -= ki;
      result.set(i, ki);
    }
    return result;
  }
  
  /**
   * Returns an integer sampled according to this distribution. Takes time proportional to np + 1. (Reference:
   * Non-Uniform Random Variate Generation, Devroye http://cgm.cs.mcgill.ca/~luc/rnbookindex.html) Second
   * time-waiting algorithm.
   */
  public static int rBinomial(int n, double p) {
    if (p >= 1.0) {
      return n; // needed to avoid infinite loops and negative results
    }
    double q = -Math.log(1.0 - p);
    double sum = 0.0;
    int x = 0;
    while (sum <= q) {
      double u = UncommonDistributions.RANDOM.nextDouble();
      double e = -Math.log(u);
      sum += e / (n - x);
      x++;
    }
    if (x == 0) {
      return 0;
    }
    return x - 1;
  }
  
  /**
   * Sample from a Dirichlet distribution, returning a vector of probabilities using a stick-breaking
   * algorithm
   * 
   * @param totalCounts
   *          an unnormalized count Vector
   * @param alpha0
   *          a double
   * @return a Vector of probabilities
   */
  public static Vector rDirichlet(Vector totalCounts, double alpha0) {
    Vector pi = totalCounts.like();
    double total = totalCounts.zSum();
    double remainder = 1.0;
    for (int k = 0; k < pi.size(); k++) {
      double countK = totalCounts.get(k);
      total -= countK;
      double betaK = rBeta(1.0 + countK, Math.max(0.0, alpha0 + total));
      double piK = betaK * remainder;
      pi.set(k, piK);
      remainder -= piK;
    }
    return pi;
  }
  
}
