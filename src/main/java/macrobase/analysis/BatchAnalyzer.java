package macrobase.analysis;

import com.google.common.base.Stopwatch;

import com.google.common.collect.Sets;
import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.compare.CubingComparer;
import macrobase.analysis.summary.compare.DecisionTreeComparer;
import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;

import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzer.class);

    public AnalysisResult analyze(SQLLoader loader,
                                  List<String> attributes,
                                  List<String> lowMetrics,
                                  List<String> highMetrics,
                                  String baseQuery) throws SQLException, IOException {
        DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();

        // OUTLIER ANALYSIS

        log.debug("Starting loading...");
        sw.start();
        List<Datum> data = loader.getData(encoder,
                                          attributes,
                                          lowMetrics,
                                          highMetrics,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        Stopwatch tsw = Stopwatch.createUnstarted();
        tsw.start();

        sw.start();
        int metricsDimensions = lowMetrics.size() + highMetrics.size();
        OutlierDetector detector = constructDetector(metricsDimensions);

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
            or = detector.classifyBatchByPercentile(data, TARGET_PERCENTILE);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, ZSCORE);
        }
        sw.stop();

        List<Set<Integer>> in_txns = new ArrayList<>();
        for(DatumWithScore d : or.getInliers()) {
            in_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
        }
        List<Set<Integer>> out_txns = new ArrayList<>();
        for(DatumWithScore d : or.getOutliers()) {
            out_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
        }

        final int iterations = 5;

        for(int i = 0; i < iterations; ++i) {
            sw.start();

            FPGrowthEmerging fpg = new FPGrowthEmerging();

            fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                  or.getOutliers(),
                                                  .01,
                                                  2,
                                                  encoder);
            sw.stop();
            log.debug("fpge took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();


            System.gc();
        }
            for(int i = 0; i < iterations; ++i) {


                sw.start();
                DecisionTreeComparer dtc = new DecisionTreeComparer();
                dtc.compare(or, 100);
                sw.stop();
                log.debug("dtc took {}", sw.elapsed(TimeUnit.MICROSECONDS));
                sw.reset();

                System.gc();

            }
            for(int i = 0; i < iterations; ++i) {

            sw.start();
            FPGrowth out_fpg = new FPGrowth();
            out_fpg.getItemsets(out_txns, .01);
            FPGrowth in_fpg = new FPGrowth();
            in_fpg.getItemsets(in_txns, .01 / (out_txns.size()));
            sw.stop();
            log.debug("fpg took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            }
        for(int i = 0; i < iterations; ++i) {


            System.gc();

            sw.start();
            Apriori out_apriori = new Apriori();
            out_apriori.getItemsets(out_txns, .01);
            Apriori in_apriori = new Apriori();
            in_apriori.getItemsets(in_txns, .01);
            sw.stop();
            log.debug("apriori took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }
        for(int i = 0; i < iterations; ++i) {

            sw.start();
            CubingComparer cub = new CubingComparer();
            cub.compare(or);
            sw.stop();
            log.debug("cube took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }

        return null;
    }
}
