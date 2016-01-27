package macrobase.analysis.summary.compare;

import com.google.common.collect.Sets;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pbailis on 1/26/16.
 */
public class DataXRay {
    private static final Logger log = LoggerFactory.getLogger(DataXRay.class);


    private final double alpha = 0.5;
    private Map<Set<Integer>, Double> computedScores = new HashMap<>();
    private Set<Set<Integer>> coveredSets = new HashSet<>();

    private double cost(Set<Integer> attrs, OutlierDetector.BatchResult or) {
        int matchingInliers = countMatches(attrs, or.getInliers());
        int matchingOutliers = countMatches(attrs, or.getOutliers());

        double errorRate = matchingOutliers != 0 ? ((double)matchingInliers)/(matchingInliers+matchingOutliers) : 0;
        if(errorRate > 0 && errorRate < 1)
            return Math.log(1/alpha) + matchingOutliers*Math.log(1/errorRate) + matchingInliers*Math.log(1/(1-errorRate));
        // matches Xray code
        else
            return Math.log(1/alpha);
    }

    private int countMatches(Set<Integer> attrs, List<DatumWithScore> data) {
        int cnt = 0;
        for(DatumWithScore i : data) {
            boolean matched = true;
            for(Integer attr : attrs) {
                if(!i.getDatum().getAttributes().contains(attr)) {
                    matched = false;
                    break;
                }
            }

            if(matched) {
                cnt += 1;
            }
        }

        return cnt;
    }

    Map<Integer, Set<Integer>> allAttributes = new HashMap<>();

    private class Candidate {
        Set<Integer> attributeSet;
        Set<Integer> dimsUsed;
    }

    private Set<Candidate> getChildren(Set<Integer> boundDimensions, Set<Integer> baseSet) {
        Set<Candidate> ret = Sets.newHashSet();
        for(Integer dim : allAttributes.keySet()) {
            if(boundDimensions.contains(dim)) {
                continue;
            }

            for(Integer attr : allAttributes.get(dim)) {
                Set<Integer> newSet = Sets.newHashSet(baseSet);
                newSet.add(attr);
                if(!coveredSets.contains(newSet)) {
                    Candidate c = new Candidate();
                    c.attributeSet = newSet;
                    Set<Integer> dimSet = Sets.newHashSet(boundDimensions);
                    dimSet.add(dim);
                    c.dimsUsed = dimSet;
                    ret.add(c);
                }
            }
        }

        return ret;
    }

    public void compare(OutlierDetector.BatchResult or) {

        final int attrSize = or.getInliers().get(0).getDatum().getAttributes().size();
        for (int i = 0; i < attrSize; ++i) {
            allAttributes.put(i, new HashSet<>());
        }

        for (DatumWithScore d : or.getInliers()) {
            List<Integer> attrs = d.getDatum().getAttributes();
            for (int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        for (DatumWithScore d : or.getOutliers()) {
            List<Integer> attrs = d.getDatum().getAttributes();
            for (int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        Set<Integer> rootSet = new HashSet<>();
        double rootScore = cost(rootSet, or);
        computedScores.put(rootSet, rootScore);

        recurse(rootScore, new HashSet<>(), new HashSet<>(), or, true);

        log.debug("Processed {} candidates", computedScores.size());
    }


    private void recurse(double parentScore,
                         Set<Integer> boundDimensions,
                         Set<Integer> currentAttributes,
                         OutlierDetector.BatchResult or,
                         boolean first) {


        Set<Candidate> children = getChildren(boundDimensions, currentAttributes);

        if(children.size() == 0) {
            return;
        }

        double childSum = 0;
        for(Candidate child : children) {
            Double childScore = computedScores.get(child.attributeSet);
            if(childScore == null) {
                childScore = cost(child.attributeSet, or);
                computedScores.put(child.attributeSet, childScore);
            }

            childSum += childScore;
        }

        if(! first && childSum > parentScore) {
            for(Candidate child : children) {
               coveredSets.add(child.attributeSet);
            }
        } else {
            for(Candidate child : children) {
                if(!coveredSets.contains(child.attributeSet)) {
                    recurse(computedScores.get(child.attributeSet),
                            child.dimsUsed,
                            child.attributeSet,
                            or,
                            false);
                }
            }
        }
    }
}