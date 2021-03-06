package macrobase.analysis.summary.itemset;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import macrobase.MacroBase;
import macrobase.analysis.summary.count.ApproximateCount;
import macrobase.analysis.summary.count.DirectCountWithThreshold;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;

/**
 * Created by pbailis on 12/26/15.
 */
public class ExponentiallyDecayingEmergingItemsets {
    private static final Logger log = LoggerFactory.getLogger(ExponentiallyDecayingEmergingItemsets.class);

    private static final Timer inlierDecayTime = MacroBase.metrics.timer(
            name(ExponentiallyDecayingEmergingItemsets.class, "inlierDecayTime"));
    private static final Timer outlierDecayTime = MacroBase.metrics.timer(
            name(ExponentiallyDecayingEmergingItemsets.class, "outlierDecayTime"));


    @SuppressWarnings("unused")
	private final int sizeOutlierSS;
    @SuppressWarnings("unused")
	private final int sizeInlierSS;

    private final double minSupportOutlier;
    private final double minRatio;
    private final double exponentialDecayRate;

    private final ApproximateCount outlierCountSummary;
    private final ApproximateCount inlierCountSummary;
    private final StreamingFPGrowth outlierPatternSummary;
    private final StreamingFPGrowth inlierPatternSummary = new StreamingFPGrowth(0);

    public ExponentiallyDecayingEmergingItemsets(int inlierSummarySize,
                                                 int outlierSummarySize,
                                                 double minSupportOutlier,
                                                 double minRatio,
                                                 double exponentialDecayRate) {
        this.sizeOutlierSS = outlierSummarySize;
        this.sizeInlierSS = inlierSummarySize;
        this.minSupportOutlier = minSupportOutlier;
        this.minRatio = minRatio;
        this.exponentialDecayRate = exponentialDecayRate;

        outlierCountSummary = new DirectCountWithThreshold(minSupportOutlier/.1); //new SpaceSaving(sizeOutlierSS);
        inlierCountSummary = new DirectCountWithThreshold(minSupportOutlier/.1);//new SpaceSaving(sizeInlierSS);
        outlierPatternSummary = new StreamingFPGrowth(minSupportOutlier);
    }

    Map<Integer, Double> interestingItems;

    public Double getInlierCount() { return inlierCountSummary.getTotalCount(); }
    public Double getOutlierCount() { return outlierCountSummary.getTotalCount(); }

    public void updateModelsNoDecay() {
        updateModels(false);
    }

    public void updateModelsAndDecay() {
        updateModels(true);
    }

    private void updateModels(boolean doDecay) {
        Map<Integer, Double> outlierCounts = this.outlierCountSummary.getCounts();
        Map<Integer, Double> inlierCounts = this.inlierCountSummary.getCounts();

        int supportCountRequired = (int)(this.outlierCountSummary.getTotalCount()*minSupportOutlier);

        interestingItems = new HashMap<>();

        for(Map.Entry<Integer, Double> outlierCount : outlierCounts.entrySet()) {
            if(outlierCount.getValue() < supportCountRequired) {
                continue;
            }

            Double inlierCount = inlierCounts.get(outlierCount.getKey());

            if(inlierCount != null &&
               ((outlierCount.getValue()/ this.outlierCountSummary.getTotalCount() /
                 (inlierCount/ this.inlierCountSummary.getTotalCount()) < minRatio))) {
                continue;
            }

            interestingItems.put(outlierCount.getKey(), outlierCount.getValue());
        }

        log.trace("found {} interesting items", interestingItems.size());

        Timer.Context ot = outlierDecayTime.time();
        outlierPatternSummary.decayAndResetFrequentItems(interestingItems, doDecay ? exponentialDecayRate : 0);
        ot.stop();

        Timer.Context it = inlierDecayTime.time();
        inlierPatternSummary.decayAndResetFrequentItems(interestingItems, doDecay ? exponentialDecayRate : 0);
        it.stop();
    }

    public void markPeriod() {
        outlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate);
        inlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate);

       updateModelsAndDecay();
    }

    public void markOutlier(Datum outlier) {
        outlierCountSummary.observe(outlier.getAttributes());
        outlierPatternSummary.insertTransactionStreamingFalseNegative(outlier.getAttributes());
    }

    // TODO: don't track *all* inliers
    public void markInlier(Datum inlier) {
        inlierCountSummary.observe(inlier.getAttributes());
        inlierPatternSummary.insertTransactionStreamingFalseNegative(inlier.getAttributes());
    }

    public List<ItemsetResult> getItemsets(DatumEncoder encoder) {
        List<ItemsetWithCount> iwc = outlierPatternSummary.getItemsets();

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Double.compare(x.getCount(), y.getCount()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        Set<Integer> ratioItemsToCheck = new HashSet<>();
        List<ItemsetWithCount> ratioSetsToCheck = new ArrayList<>();
        List<ItemsetResult> ret = new ArrayList<>();

        Set<Integer> prevSet = null;
        Double prevCount = -1.;
        for(ItemsetWithCount i : iwc) {
            if(i.getCount() == prevCount) {
                if(prevSet != null && Sets.difference(i.getItems(), prevSet).size() == 0) {
                    continue;
                }
            }


            prevCount = i.getCount();
            prevSet = i.getItems();


            if(i.getItems().size() == 1) {
                double ratio = 0;
                int item = i.getItems().iterator().next();
                double inlierCount = inlierCountSummary.getCount(item);

                if(inlierCount > 0) {
                    ratio = (outlierCountSummary.getCount(item)/ outlierCountSummary.getTotalCount())/
                            (inlierCount/ inlierCountSummary.getTotalCount());
                } else {
                    ratio = Double.POSITIVE_INFINITY;
                }

                ret.add(new ItemsetResult(i.getCount()/ outlierCountSummary.getTotalCount(),
                                          i.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(i.getItems())));
            } else {
                ratioItemsToCheck.addAll(i.getItems());
                ratioSetsToCheck.add(i);
            }
        }

        // check the ratios of any itemsets we just marked
        List<ItemsetWithCount> matchingInlierCounts = inlierPatternSummary.getCounts(ratioSetsToCheck);

        assert(matchingInlierCounts.size() == ratioSetsToCheck.size());
        for(int i = 0; i < matchingInlierCounts.size(); ++i) {
            ItemsetWithCount ic = matchingInlierCounts.get(i);
            ItemsetWithCount oc = ratioSetsToCheck.get(i);

            double ratio;
            if(ic.getCount() > 0) {
                ratio = (oc.getCount()/ outlierCountSummary.getTotalCount())/(ic.getCount()/ inlierCountSummary.getTotalCount());
            } else {
                ratio = Double.POSITIVE_INFINITY;
            }

            if(ratio >= minRatio) {
                ret.add(new ItemsetResult(oc.getCount()/ outlierCountSummary.getTotalCount(),
                                          oc.getCount(),
                                          ratio,
                                          encoder.getColsFromAttrSet(oc.getItems())));
            }
        }

        // finally sort one last time
        ret.sort((x, y) -> x.getNumRecords() != y.getNumRecords() ?
                -Double.compare(x.getNumRecords(), y.getNumRecords()) :
                -Double.compare(x.getItems().size(), y.getItems().size()));

        return ret;

    }
}
