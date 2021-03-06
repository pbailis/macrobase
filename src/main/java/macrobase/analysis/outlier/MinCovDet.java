package macrobase.analysis.outlier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import macrobase.datamodel.Datum;
import macrobase.datamodel.HasMetrics;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinCovDet extends OutlierDetector  {
    private static final Logger log = LoggerFactory.getLogger(MinCovDet.class);

    class MetricsWithScore implements HasMetrics {
        private RealVector metrics;
        private Double score;
        private Integer originalPosition;

        public MetricsWithScore(RealVector metrics,
                                double score,
                                Integer originalPosition) {
            this.metrics = metrics;
            this.score = score;
            this.originalPosition = originalPosition;
        }

        public RealVector getMetrics() {
            return metrics;
        }

        public Double getScore() {
            return score;
        }

        public Integer getOriginalPosition() {
            return originalPosition;
        }
    }

    // p == dataset dimension
    private final int p;
    // H = alpha*(n+p+1)
    private double alpha = .5;
    private Random random = new Random();
    private double stoppingDelta = 1e-3;

    private RealMatrix cov;
    private RealVector mean;

    // efficient only when k << allData.size()
    private List<Datum> chooseKRandom(List<Datum> allData, final int k) {
        assert(k < allData.size());

        List<Datum> ret = new ArrayList<>();
        Set<Integer> alreadyChosen = new HashSet<>();
        int remaining = k;
        while(remaining > 0) {
            int idx = random.nextInt(allData.size());
            if(!alreadyChosen.contains(idx)) {
                alreadyChosen.add(idx);
                ret.add(allData.get(idx));
            }
            remaining -= 1;
        }

        assert(ret.size() == k);
        return ret;
    }

    public MinCovDet(int dataDim) {
        this.p = dataDim;
    }

    public MinCovDet(int dataDim, double alpha) {
        this(dataDim);
        this.alpha = alpha;
    }

    public static double getMahalanobis(RealVector mean,
                                        RealMatrix cov,
                                        RealVector vec) {
        // sqrt((vec-mean)^T S^-1 (vec-mean))
        RealMatrix vecT = new Array2DRowRealMatrix(vec.toArray());
        RealMatrix meanT = new Array2DRowRealMatrix(mean.toArray());
        RealMatrix vecSubtractMean = vecT.subtract(meanT);

        RealMatrix covInverse = new LUDecomposition(cov).getSolver().getInverse();
        return Math.sqrt(vecSubtractMean.transpose()
                                 .multiply(covInverse)
                                 .multiply(vecSubtractMean).getEntry(0, 0));
    }

    private RealVector getMean(List<? extends HasMetrics> data) {
        RealVector vec = null;

        for(HasMetrics d : data) {
            RealVector dvec = d.getMetrics();
            if(vec == null) {
                vec = dvec;
            } else {
                vec = vec.add(dvec);
            }
        }

        return vec.mapDivide(data.size());
    }

    private RealMatrix getCovariance(List<? extends HasMetrics> data) {
        int p = data.iterator().next().getMetrics().getDimension();
        RealMatrix ret = new Array2DRowRealMatrix(data.size(), p);
        int idx = 0;
        for(HasMetrics d : data) {
            ret.setRow(idx, d.getMetrics().toArray());
            idx += 1;
        }

        return (new Covariance(ret)).getCovarianceMatrix();
    }

    private List<MetricsWithScore> findKClosest(int k,
                                          List<? extends HasMetrics> data,
                                          RealVector mean,
                                          RealMatrix cov) {
        // todo: change back to guava priority queue
        List<MetricsWithScore> scores = new ArrayList<>();

        for(int i = 0; i < data.size(); ++i) {
            HasMetrics d = data.get(i);
            scores.add(new MetricsWithScore(d.getMetrics(),
                                            getMahalanobis(mean, cov, d.getMetrics()),
                                            i));
        }

        scores.sort((a, b) -> a.getScore().compareTo(b.getScore()));

        return scores.subList(0, k);
    }

    // helper method
    public static double getDeterminant(RealMatrix cov) {
        return (new LUDecomposition(cov)).getDeterminant();
    }

    @Override
    public void train(List<Datum> data) {
        // for now, only handle multivariate case...
        assert(data.iterator().next().getMetrics().getDimension() == p);
        assert(p > 1);

        int h = (int)Math.floor((data.size() + p + 1)*alpha);

        // select initial dataset
        List<? extends HasMetrics> initialSubset = chooseKRandom(data, p + 1);
        mean = getMean(initialSubset);
        cov = getCovariance(initialSubset);
        double det = getDeterminant(cov);

        int stepNo = 1;

        // now take C-steps
        while(true) {
            List<? extends HasMetrics> newH = findKClosest(h, data, mean, cov);
            mean = getMean(newH);
            cov = getCovariance(newH);

            double newDet = getDeterminant(cov);
            double delta = det - newDet;

            if(newDet == 0 || delta < stoppingDelta) {
                break;
            }

            log.trace("Iteration {}: delta = {}; det = {}", stepNo, delta, newDet);
            det = newDet;
            stepNo++;
        }

        log.trace("mean: {}", mean);
        log.trace("cov: {}", cov);
    }

    @Override
    public double score(Datum datum) {
        return getMahalanobis(mean, cov, datum.getMetrics());
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // compute zscore to CDF
        double cdf = (new NormalDistribution()).cumulativeProbability(zscore);
        // for normal distribution, mahalanobis distance is chi-squared
        // https://en.wikipedia.org/wiki/Mahalanobis_distance#Normal_distributions
        return (new ChiSquaredDistribution(p)).inverseCumulativeProbability(cdf);
    }
}
