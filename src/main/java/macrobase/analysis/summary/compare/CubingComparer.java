package macrobase.analysis.summary.compare;

import com.google.common.collect.Sets;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pbailis on 1/25/16.
 */
public class CubingComparer {
    private static final Logger log = LoggerFactory.getLogger(CubingComparer.class);

    private Map<double[], Double> cubeCount = new HashMap<>();

    private final int WILDCARD = -0xDEADBEEF;

    private boolean hasMatch(double[] pattern, DatumWithScore d) {
        for (int i = 0; i < pattern.length; ++i) {
            if(pattern[i] != WILDCARD && d.getDatum().getAttributes().get(i) != pattern[i]) {
                return false;
            }
        }

        return true;
    }

    public double getSupportRatio(double[] pattern, List<DatumWithScore> inliers, List<DatumWithScore> outliers) {
        double inlierCount = 0, outlierCount = 0;
        for(DatumWithScore d : inliers) {
            if(hasMatch(pattern, d)) {
                inlierCount += 1;
            }
        }

        for(DatumWithScore d : outliers) {
            if(hasMatch(pattern, d)) {
                outlierCount += 1;
            }
        }

        log.debug("{}: {} {}", pattern, outlierCount, inlierCount);

        return outlierCount/inlierCount;
    }

    Map<Integer, Set<Integer>> allAttributes = new HashMap<>();

    public void compare(OutlierDetector.BatchResult or) {
        List<DatumWithScore> inliers = or.getInliers();
        List<DatumWithScore> outliers = or.getOutliers();


        final int attrSize = inliers.get(0).getDatum().getAttributes().size();
        for(int i = 0; i < attrSize; ++i) {
            allAttributes.put(i, new HashSet<>());
        }

        for(DatumWithScore d : inliers) {
            List<Integer> attrs = d.getDatum().getAttributes();
            for(int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        for(DatumWithScore d : outliers) {
            List<Integer> attrs = d.getDatum().getAttributes();
            for(int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        double[] init = new double[attrSize];
        Arrays.fill(init, WILDCARD);

        Set<double[]> prevRoundPatterns = Sets.newHashSet(init);
        int cnt = 0;
        while(true) {
            cnt += 1;
            int curSize = cubeCount.size();
            prevRoundPatterns = computeNewCubes(prevRoundPatterns, inliers, outliers);
            log.debug("round {}", cnt);
            if(cubeCount.size() == curSize) {
                break;
            }
        }

        log.info("Cubed in {} iterations", cnt);
    }

    private Set<double[]>  computeNewCubes(Set<double[]> prevRoundPatterns, List<DatumWithScore> inliers, List<DatumWithScore> outliers) {
        HashSet<double[]> thisRoundPatterns = new HashSet<>();
        for(int i = 0; i < allAttributes.size(); ++i) {
            for(double[] pattern : prevRoundPatterns) {
                if(pattern[i] == WILDCARD) {
                    for(Integer attr : allAttributes.get(i)) {
                        double[] newPattern = pattern.clone();
                        newPattern[i] = attr;
                        cubeCount.put(newPattern, getSupportRatio(newPattern, inliers, outliers));
                        thisRoundPatterns.add(newPattern);
                    }
                }
            }
        }

        return thisRoundPatterns;
    }
}
