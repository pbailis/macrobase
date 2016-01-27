package macrobase.analysis;

import com.google.common.base.Stopwatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.result.AnalysisResult;
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
import java.util.stream.Collectors;

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

final double pctile = .75;

        OutlierDetector gold = new MAD();
        gold.train(data);
        OutlierDetector.BatchResult goldResult = gold.classifyBatchByPercentile(data, pctile);
        Set<Datum> goldOutliers = Sets.newHashSet(
                goldResult.getOutliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));
        Set<Datum> goldInliers = Sets.newHashSet(goldResult.getInliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));


        Map<Datum, Double> goldScores = new HashMap<>();
        double goldTotal = 0;
        for(DatumWithScore d : goldResult.getInliers()) {
            goldScores.put(d.getDatum(), d.getScore());
            goldTotal += d.getScore();
        }

        for(DatumWithScore d : goldResult.getOutliers()) {
            goldScores.put(d.getDatum(), d.getScore());
            goldTotal += d.getScore();
        }

        double[] l = {.00001, .0001, .001, .01, .1, .5, 1};
        final int ITERATIONS = 10;

        for(Double h : l) {
            List<Double> precisions = new ArrayList<>();
            List<Double> recalls = new ArrayList<>();
            List<Double> times = new ArrayList<>();
            List<Double> rmses = new ArrayList<>();


            for(int i = 0; i < ITERATIONS; ++i) {
                OutlierDetector detector = new MAD();
                Random random = new Random();
                Collections.shuffle(data);

                List<Datum> sample = data.subList(0, (int) (data.size() * h));

                log.debug("Sample size is {}", sample.size());

                sw.reset();
                sw.start();
                detector.train(sample);
                sw.stop();
                times.add((double)sw.elapsed(TimeUnit.MICROSECONDS));

                OutlierDetector.BatchResult curResult = detector.classifyBatchByPercentile(data, pctile);
                Set<Datum> curOutliers = Sets.newHashSet(
                        curResult.getOutliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));
                Set<Datum> curInliers = Sets.newHashSet(
                        curResult.getInliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));

                double sum_squares = 0;
                for(DatumWithScore d : curResult.getInliers()) {
                    sum_squares += Math.pow(goldScores.get(d.getDatum()) - d.getScore(), 2);
                }
                for(DatumWithScore d : curResult.getOutliers()) {
                    sum_squares += Math.pow(goldScores.get(d.getDatum()) - d.getScore(), 2);
                }

                double rmse = Math.sqrt(sum_squares/data.size());

                log.info("RMSE: {}", rmse);

                log.info("minscore: {} {}",
                         curResult.getOutliers().get(0).getScore(),
                         goldResult.getOutliers().get(0).getScore());


                log.info("maxscore: {} {}",
                         curResult.getOutliers().get(curOutliers.size()-1).getScore(),
                         goldResult.getOutliers().get(goldOutliers.size()-1).getScore());

                double precision = (double) Sets.intersection(curOutliers, goldOutliers).size() / curOutliers.size();
                log.info("intersection size is {} {} {}", Sets.intersection(curOutliers, goldOutliers).size(), curOutliers.size(), goldOutliers.size());
                double recall = (double) Sets.intersection(curOutliers, goldOutliers).size() / goldOutliers.size();
                precisions.add(precision);
                recalls.add(recall);
                rmses.add(rmse);
            }

            double avgp = precisions.stream().reduce((a, b) -> a+b).get()/precisions.size();
            double avgr = recalls.stream().reduce((a, b) -> a+b).get()/recalls.size();
            double avgtime = times.stream().reduce((a, b) -> a+b).get()/times.size();
            double avgrmse = rmses.stream().reduce((a, b) -> a+b).get()/rmses.size();


            log.info("h: {}, avgprecision: {}, avgrecall: {}, avgtime:{}, avgrmse: {}", h, avgp, avgr, avgtime, avgrmse);
            log.info("h: {}, precisions: {}, recalls: {}, times:{}, rmses: {}", h, precisions, recalls, times, rmses);



        }

        /*
         gold = new ZScore();
        gold.train(data);
        goldResult = gold.classifyBatchByPercentile(data, pctile);
        goldOutliers = Sets.newHashSet(
                goldResult.getOutliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));
        goldInliers = Sets.newHashSet(goldResult.getInliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));
*/

        for(Double h : l) {
            List<Double> precisions = new ArrayList<>();
            List<Double> recalls = new ArrayList<>();
            List<Double> times = new ArrayList<>();

            for(int i = 0; i < ITERATIONS; ++i) {
                OutlierDetector detector = new ZScore();
                Random random = new Random();
                Collections.shuffle(data);

                List<Datum> sample = data.subList(0, (int) (data.size() * h));

                sw.reset();
                sw.start();
                detector.train(sample);
                sw.stop();
                times.add((double)sw.elapsed(TimeUnit.MICROSECONDS));

                OutlierDetector.BatchResult curResult = detector.classifyBatchByPercentile(data, pctile);
                Set<Datum> curOutliers = Sets.newHashSet(
                        curResult.getOutliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));
                Set<Datum> curInliers = Sets.newHashSet(
                        curResult.getInliers().stream().map(a -> a.getDatum()).collect(Collectors.toList()));

                double precision = (double) Sets.intersection(curOutliers, goldOutliers).size() / curOutliers.size();
                double recall = (double) Sets.intersection(curOutliers, goldOutliers).size() / goldOutliers.size();
                precisions.add(precision);
                recalls.add(recall);
            }

            double avgp = precisions.stream().reduce((a, b) -> a+b).get()/precisions.size();
            double avgr = recalls.stream().reduce((a, b) -> a+b).get()/recalls.size();
            double avgtime = times.stream().reduce((a, b) -> a+b).get()/times.size();


            log.info("ZSCORE h: {}, avgprecision: {}, avgrecall: {}, avgtime:{}", h, avgp, avgr, avgtime);
            log.info("ZSCORE h: {}, avgprecision: {}, avgrecall: {}, avgtime:{}", h, precisions, recalls, times);



        }


        if(true)
            return null;

        OutlierDetector detector = constructDetector(metricsDimensions);

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
            or = detector.classifyBatchByPercentile(data, TARGET_PERCENTILE);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, ZSCORE);
        }
        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        // SUMMARY

        @SuppressWarnings("unused")
		final int supportCountRequired = (int) MIN_SUPPORT*or.getOutliers().size();

        final int inlierSize = or.getInliers().size();
        final int outlierSize = or.getOutliers().size();

        log.debug("Starting summarization...");

        sw.start();
        FPGrowthEmerging fpg = new FPGrowthEmerging();
        List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                        or.getOutliers(),
                                                                        MIN_SUPPORT,
                                                                        MIN_INLIER_RATIO,
                                                                        encoder);
        sw.stop();
        tsw.stop();
        
        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        
        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended summarization (time: {}ms)!", summarizeTime);

        log.debug("Number of itemsets: {}", isr.size());
        log.debug("...ended total (time: {}ms)!", (tsw.elapsed(TimeUnit.MICROSECONDS) / 1000) + 1);
        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}
