package macrobase.analysis.summary.compare;

import com.google.common.collect.Sets;
import macrobase.analysis.summary.result.DatumWithScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pbailis on 1/25/16.
 */
public class CubingComparer {
    private static final Logger log = LoggerFactory.getLogger(CubingComparer.class);

    Map<Set<Integer>, Integer> cnt = new HashMap<>();

    public Map<Set<Integer>, Integer> compare(List<DatumWithScore> data) {
        for(DatumWithScore d : data) {
            Set<Integer> as = Sets.newHashSet(d.getDatum().getAttributes());
            for(Set<Integer> ss : Sets.powerSet(as)) {
                if(ss.size() == 0) {
                    continue;
                }

                cnt.compute(ss, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        return cnt;
    }
}
