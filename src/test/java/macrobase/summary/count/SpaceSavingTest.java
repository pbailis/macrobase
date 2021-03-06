package macrobase.summary.count;

/**
 * Created by pbailis on 12/24/15.
 */

import macrobase.analysis.summary.count.SpaceSaving;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpaceSavingTest {

    @Test
    public void simpleTest() {
        SpaceSaving ss = new SpaceSaving(10);
        ss.observe(1);
        ss.observe(1);
        ss.observe(1);
        ss.observe(2);
        ss.observe(3);
        ss.observe(1);
        ss.observe(3);
        ss.observe(2);
        ss.observe(3);

        ss.debugPrint();

        assertEquals(4, ss.getCount(1), 0);
        assertEquals(2, ss.getCount(2), 0);
        assertEquals(3, ss.getCount(3), 0);
    }

    @Test
    public void overflowTest() {
        SpaceSaving ss = new SpaceSaving(10);

        for(int i = 0; i < 10; ++i) {
            ss.observe(i);
            assertEquals(1, ss.getCount(i), 0);
        }

        ss.observe(10);
        assertEquals(2, ss.getCount(10), 0);
    }
}
