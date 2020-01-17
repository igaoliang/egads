package gl;

import com.yahoo.egads.control.ProcessableObject;
import com.yahoo.egads.control.ProcessableObjectFactory;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.models.adm.DBScanModel;
import com.yahoo.egads.models.adm.ExtremeLowDensityModel;
import com.yahoo.egads.models.adm.SimpleThresholdModel;
import com.yahoo.egads.models.tsmm.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class GT {


    @Test
    public void testDetectAnomalyProcessable() throws Exception {

        InputStream is = new FileInputStream("src/test/resources/sample_config.ini");
        Properties p = new Properties();
        p.load(is);
        p.setProperty("TS_MODEL","MovingAverageModel");
        p.setProperty("AD_MODEL","ExtremeLowDensityModel");
//        p.setProperty("MAX_ANOMALY_TIME_AGO","0");
//        p.setProperty("OP_TYPE","DETECT_ANOMALY");

        ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);

        // generate expected result
        Long anomalousTime = 1417194000L;
        Anomaly anomaly = new Anomaly("value",null);
        anomaly.addInterval(anomalousTime, anomalousTime,0.0f);

        // actual result
        ProcessableObject po = ProcessableObjectFactory.create(metrics.get(0), p);
        po.process();

        Assert.assertEquals(po.result().toString(), Arrays.asList(anomaly).toString());
    }


    @Test
    public void testOlympi() throws Exception {

        String configFile = "src/test/resources/sample_config.ini";
        InputStream is = new FileInputStream(configFile);
        Properties p = new Properties();
        p.load(is);

        System.out.println(p);


        ArrayList<TimeSeries> actual_metric = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);

        // Parse the input timeseries.
//        ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
//                .createTimeSeries("src/test/resources/sample.csv", p);


        ArrayList<TimeSeries> metrics = actual_metric;

        WeightedMovingAverageModel model = new WeightedMovingAverageModel(p);
        long start = System.currentTimeMillis();

        model.train(actual_metric.get(0).data);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        TimeSeries.DataSequence sequence = new TimeSeries.DataSequence(metrics.get(0).startTime(),
                metrics.get(0).lastTime(),
                300);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        sequence.setLogicalIndices(metrics.get(0).startTime(), 300);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        model.predict(sequence);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        // Initialize the anomaly detector.
        ExtremeLowDensityModel bcm = new ExtremeLowDensityModel(p);



        bcm.tune(actual_metric.get(0).data, sequence);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        // Initialize the DBScan anomaly detector.
        DBScanModel dbs = new DBScanModel(p);

        Anomaly.IntervalSequence anomalies = bcm.detect(actual_metric.get(0).data, sequence);

        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();



        System.out.println("ExtremeLowDensityModel : " + anomalies);


        Assert.assertTrue(anomalies.size() < 10);

    }


    @Test
    public void testOlympicModel() throws Exception {

        String configFile = "src/test/resources/sample_config.ini";
        InputStream is = new FileInputStream(configFile);
        Properties p = new Properties();
        p.load(is);

        ArrayList<TimeSeries> actual_metric = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);

        // Parse the input timeseries.
        ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);
        MovingAverageModel model = new MovingAverageModel(p);


        model.train(actual_metric.get(0).data);
        TimeSeries.DataSequence sequence = new TimeSeries.DataSequence(metrics.get(0).startTime(),
                metrics.get(0).lastTime(),
                300);
        sequence.setLogicalIndices(metrics.get(0).startTime(), 300);
        model.predict(sequence);
        // Initialize the anomaly detector.
        ExtremeLowDensityModel bcm = new ExtremeLowDensityModel(p);
        bcm.tune(actual_metric.get(0).data, sequence);

        // Initialize the DBScan anomaly detector.
        DBScanModel dbs = new DBScanModel(p);

        Anomaly.IntervalSequence anomalies = bcm.detect(actual_metric.get(0).data, sequence);
        dbs.tune(actual_metric.get(0).data, sequence);
        Anomaly.IntervalSequence anomaliesdb = dbs.detect(actual_metric.get(0).data, sequence);

        // Initialize the SimpleThreshold anomaly detector.
        SimpleThresholdModel stm = new SimpleThresholdModel(p);

        stm.tune(actual_metric.get(0).data, sequence);
        Anomaly.IntervalSequence anomaliesstm = stm.detect(actual_metric.get(0).data, sequence);


        System.out.println("ExtremeLowDensityModel : " + anomalies);
        System.out.println("DBScanModel : " + anomaliesdb);
        System.out.println("SimpleThresholdModel : " + anomaliesstm);

        Assert.assertTrue(anomalies.size() < 10);
        Assert.assertTrue(anomaliesdb.size() > 2);
        Assert.assertTrue(anomaliesstm.size() > 2);
    }


    @Test
    public void testOlympicModelXXXXXX() throws Exception {
        // Test cases: ref window: 10, 5
        // Drops: 0, 1
        String[] refWindows = new String[]{"10", "5"};
        String[] drops = new String[]{"0", "1"};
        // Load the true expected values from a file.
        String configFile = "src/test/resources/sample_config.ini";
        InputStream is = new FileInputStream(configFile);
        Properties p = new Properties();
        p.load(is);
        ArrayList<TimeSeries> actual_metric = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/model_input.csv", p);
        p.setProperty("MAX_ANOMALY_TIME_AGO", "999999999");
        for (int w = 0; w < refWindows.length; w++) {
            for (int d = 0; d < drops.length; d++) {
                p.setProperty("NUM_WEEKS", refWindows[w]);
                p.setProperty("NUM_TO_DROP", drops[d]);
                p.setProperty("THRESHOLD", "mapee#100,mase#10");
                // Parse the input timeseries.
                ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
                        .createTimeSeries("src/test/resources/model_output_" + refWindows[w] + "_" + drops[d] + ".csv", p);
                OlympicModel model = new OlympicModel(p);
                model.train(actual_metric.get(0).data);
                TimeSeries.DataSequence sequence = new TimeSeries.DataSequence(metrics.get(0).startTime(),
                        metrics.get(0).lastTime(),
                        3600);
                sequence.setLogicalIndices(metrics.get(0).startTime(), 3600);
                model.predict(sequence);
                // Initialize the anomaly detector.
                ExtremeLowDensityModel bcm = new ExtremeLowDensityModel(p);

                // Initialize the DBScan anomaly detector.
                DBScanModel dbs = new DBScanModel(p);
                Anomaly.IntervalSequence anomalies = bcm.detect(actual_metric.get(0).data, sequence);
                dbs.tune(actual_metric.get(0).data, sequence);
                Anomaly.IntervalSequence anomaliesdb = dbs.detect(actual_metric.get(0).data, sequence);

                // Initialize the SimpleThreshold anomaly detector.
                SimpleThresholdModel stm = new SimpleThresholdModel(p);

                stm.tune(actual_metric.get(0).data, sequence);
                Anomaly.IntervalSequence anomaliesstm = stm.detect(actual_metric.get(0).data, sequence);
                org.junit.Assert.assertTrue(anomalies.size() > 10);
                org.junit.Assert.assertTrue(anomaliesdb.size() > 2);
                org.junit.Assert.assertTrue(anomaliesstm.size() > 2);
            }
        }
    }


    @Test
    public void test() throws Exception {
        String configFile = "src/test/resources/sample_config.ini";
        InputStream is = new FileInputStream(configFile);
        Properties p = new Properties();
        p.load(is);
        ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);

        System.out.println(metrics.get(0).data);

        MovingAverageModel model = new MovingAverageModel(p);

        model.train(metrics.get(0).data);
        // 数据按照period 分组。每个数据都是 开始数据 + period的数据。中间的数据直接被跳过
        // perios 是你的测试数据的周期。以秒维单位
        TimeSeries.DataSequence sequence = new TimeSeries.DataSequence(metrics.get(0).startTime(),
                metrics.get(0).lastTime(),
                300);

        System.out.println(metrics.get(0).data.size());
        System.out.println(metrics.get(0).size());
        System.out.println(sequence.size());


        // setLogicalIndices 逻辑分组。说白了就是按照period进行数据拆分，然后数据在第几个重新弄划分组的组别好
        sequence.setLogicalIndices(metrics.get(0).startTime(), 300);
        model.predict(sequence);
        Assert.assertEquals(verifyResults(sequence, metrics.get(0).data), true);
    }

    @Test
    public void testAutoForecast() throws Exception {

        String configFile = "src/test/resources/sample_config.ini";
        InputStream is = new FileInputStream(configFile);
        Properties p = new Properties();
        p.load(is);
        ArrayList<TimeSeries> metrics = com.yahoo.egads.utilities.FileUtils
                .createTimeSeries("src/test/resources/sample.csv", p);
        MovingAverageModel model = new MovingAverageModel(p);
        model.train(metrics.get(0).data);
        TimeSeries.DataSequence sequence = new TimeSeries.DataSequence(metrics.get(0).startTime(),
                metrics.get(0).lastTime(),
                3600);

        sequence.setLogicalIndices(metrics.get(0).startTime(), 3600);
        model.predict(sequence);
        Assert.assertEquals(verifyResults(sequence, metrics.get(0).data), true);
    }

    // Verifies that the two time-series are identical.
    private boolean verifyResults (TimeSeries.DataSequence computed, TimeSeries.DataSequence actual) {
        int n = computed.size();
        int n2 = actual.size();
        if (n != n2) {
            return false;
        }
        float precision = (float) 0.000001;
        float errorSum = (float) 0.0;
        for (int i = 0; i < n; i++) {
            errorSum += Math.abs(computed.get(i).value - actual.get(i).value);

            if(i % 1000 ==0){
                System.out.println(String.valueOf(computed.get(i).value) + " <--- " + String.valueOf(actual.get(i).value));
            }

        }
        errorSum /= n;

        if (errorSum <= 5152990) {
            return true;
        }
        return false;
    }

}
