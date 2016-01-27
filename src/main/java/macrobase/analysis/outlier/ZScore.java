package macrobase.analysis.outlier;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.List;

import com.codahale.metrics.Timer;

import macrobase.MacroBase;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZScore extends OutlierDetector {
    private static final Logger log = LoggerFactory.getLogger(ZScore.class);

    private double mean;
    private double std;

    private final Timer meanComputation = MacroBase.metrics.timer(name(ZScore.class, "meanComputation"));
    private final Timer stddevComputation = MacroBase.metrics.timer(name(ZScore.class, "stddevComputation"));

    @Override
    public void train(List<Datum> data) {
        double sum = 0;

        Timer.Context context = meanComputation.time();
        for(Datum d : data) {
            assert(d.getMetrics().getDimension() == 1);
            sum += d.getMetrics().getEntry(0);
        }
        mean = sum/data.size();
        context.stop();

        context = stddevComputation.time();
        double ss = 0;
        for(Datum d : data) {
            ss += Math.pow(mean - d.getMetrics().getEntry(0), 2);
        }
        std = Math.sqrt(ss / data.size());
        context.stop();

        log.info("mean is {} std is {}", mean, std);
    }

    @Override
    public double score(Datum datum) {
        double point = datum.getMetrics().getEntry(0);
        return Math.abs(point-mean)/std;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // z-score is identity since we're literally calculating the z-score
        return zscore;
    }
}
