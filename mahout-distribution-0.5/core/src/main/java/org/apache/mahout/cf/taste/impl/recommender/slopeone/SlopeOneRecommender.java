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

import java.util.Collection;
import java.util.List;

import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.RefreshHelper;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverageAndStdDev;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.slopeone.DiffStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A basic "slope one" recommender. (See an <a href="http://www.daniel-lemire.com/fr/abstracts/SDM2005.html">
 * excellent summary here</a> for example.) This {@link org.apache.mahout.cf.taste.recommender.Recommender} is
 * especially suitable when user preferences are updating frequently as it can incorporate this information
 * without expensive recomputation.
 * </p>
 * 
 * <p>
 * This implementation can also be used as a "weighted slope one" recommender.
 * </p>
 */
public final class SlopeOneRecommender extends AbstractRecommender {
  
  private static final Logger log = LoggerFactory.getLogger(SlopeOneRecommender.class);
  
  private final boolean weighted;
  private final boolean stdDevWeighted;
  private final DiffStorage diffStorage;
  
  /**
   * <p>
   * Creates a default (weighted)  based on the given {@link DataModel}.
   * </p>
   */
  public SlopeOneRecommender(DataModel dataModel) throws TasteException {
    this(dataModel,
         Weighting.WEIGHTED,
         Weighting.WEIGHTED,
         new MemoryDiffStorage(dataModel, Weighting.WEIGHTED, Long.MAX_VALUE));
  }
  
  /**
   * <p>
   * Creates a  based on the given {@link DataModel}.
   * </p>
   *
   * <p>
   * If {@code weighted} is set, acts as a weighted slope one recommender. This implementation also
   * includes an experimental "standard deviation" weighting which weights item-item ratings diffs with lower
   * standard deviation more highly, on the theory that they are more reliable.
   * </p>
   *
   * @param weighting
   *          if {@link Weighting#WEIGHTED}, acts as a weighted slope one recommender
   * @param stdDevWeighting
   *          use optional standard deviation weighting of diffs
   * @throws IllegalArgumentException
   *           if {@code diffStorage} is null, or stdDevWeighted is set when weighted is not set
   */
  public SlopeOneRecommender(DataModel dataModel,
                             Weighting weighting,
                             Weighting stdDevWeighting,
                             DiffStorage diffStorage) {
    super(dataModel);
    Preconditions.checkArgument(stdDevWeighting != Weighting.WEIGHTED || weighting != Weighting.UNWEIGHTED,
      "weighted required when stdDevWeighted is set");
    Preconditions.checkArgument(diffStorage != null, "diffStorage is null");
    this.weighted = weighting == Weighting.WEIGHTED;
    this.stdDevWeighted = stdDevWeighting == Weighting.WEIGHTED;
    this.diffStorage = diffStorage;
  }
  
  @Override
  public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
    Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");
    log.debug("Recommending items for user ID '{}'", userID);

    FastIDSet possibleItemIDs = diffStorage.getRecommendableItemIDs(userID);

    TopItems.Estimator<Long> estimator = new Estimator(userID);

    List<RecommendedItem> topItems = TopItems.getTopItems(howMany, possibleItemIDs.iterator(), rescorer,
      estimator);

    log.debug("Recommendations are: {}", topItems);
    return topItems;
  }
  
  @Override
  public float estimatePreference(long userID, long itemID) throws TasteException {
    DataModel model = getDataModel();
    Float actualPref = model.getPreferenceValue(userID, itemID);
    if (actualPref != null) {
      return actualPref;
    }
    return doEstimatePreference(userID, itemID);
  }
  
  private float doEstimatePreference(long userID, long itemID) throws TasteException {
    double count = 0.0;
    double totalPreference = 0.0;
    PreferenceArray prefs = getDataModel().getPreferencesFromUser(userID);
    RunningAverage[] averages = diffStorage.getDiffs(userID, itemID, prefs);
    int size = prefs.length();
    for (int i = 0; i < size; i++) {
      RunningAverage averageDiff = averages[i];
      if (averageDiff != null) {
        double averageDiffValue = averageDiff.getAverage();
        if (weighted) {
          double weight = averageDiff.getCount();
          if (stdDevWeighted) {
            double stdev = ((RunningAverageAndStdDev) averageDiff).getStandardDeviation();
            if (!Double.isNaN(stdev)) {
              weight /= 1.0 + stdev;
            }
            // If stdev is NaN, then it is because count is 1. Because we're weighting by count,
            // the weight is already relatively low. We effectively assume stdev is 0.0 here and
            // that is reasonable enough. Otherwise, dividing by NaN would yield a weight of NaN
            // and disqualify this pref entirely
            // (Thanks Daemmon)
          }
          totalPreference += weight * (prefs.getValue(i) + averageDiffValue);
          count += weight;
        } else {
          totalPreference += prefs.getValue(i) + averageDiffValue;
          count += 1.0;
        }
      }
    }
    if (count <= 0.0) {
      RunningAverage itemAverage = diffStorage.getAverageItemPref(itemID);
      return itemAverage == null ? Float.NaN : (float) itemAverage.getAverage();
    } else {
      return (float) (totalPreference / count);
    }
  }
  
  @Override
  public void setPreference(long userID, long itemID, float value) throws TasteException {
    DataModel dataModel = getDataModel();
    Float oldPref;
    try {
      oldPref = dataModel.getPreferenceValue(userID, itemID);
    } catch (NoSuchUserException nsee) {
      oldPref = null;
    }
    super.setPreference(userID, itemID, value);
    if (oldPref == null) {
      // Add new preference
      diffStorage.addItemPref(userID, itemID, value);
    } else {
      // Update preference
      diffStorage.updateItemPref(itemID, value - oldPref);
    }
  }
  
  @Override
  public void removePreference(long userID, long itemID) throws TasteException {
    DataModel dataModel = getDataModel();
    Float oldPref = dataModel.getPreferenceValue(userID, itemID);
    super.removePreference(userID, itemID);
    if (oldPref != null) {
      diffStorage.removeItemPref(userID, itemID, oldPref);
    }
  }
  
  @Override
  public void refresh(Collection<Refreshable> alreadyRefreshed) {
    alreadyRefreshed = RefreshHelper.buildRefreshed(alreadyRefreshed);
    RefreshHelper.maybeRefresh(alreadyRefreshed, diffStorage);
  }
  
  @Override
  public String toString() {
    return "SlopeOneRecommender[weighted:" + weighted + ", stdDevWeighted:" + stdDevWeighted
           + ", diffStorage:" + diffStorage + ']';
  }
  
  private final class Estimator implements TopItems.Estimator<Long> {
    
    private final long userID;
    
    private Estimator(long userID) {
      this.userID = userID;
    }
    
    @Override
    public double estimate(Long itemID) throws TasteException {
      return doEstimatePreference(userID, itemID);
    }
  }
  
}
