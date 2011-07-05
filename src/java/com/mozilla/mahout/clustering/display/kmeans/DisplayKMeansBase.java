package com.mozilla.mahout.clustering.display.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;

import com.mozilla.hadoop.fs.SequenceFileDirectoryReader;

public class DisplayKMeansBase {

    private static final Logger LOG = Logger.getLogger(DisplayKMeansBase.class);
    
    public List<Pair<Integer,Vector>> readClusteredPoints(Path clusteredPointsPath) {
        List<Pair<Integer,Vector>> clusteredPoints = new ArrayList<Pair<Integer,Vector>>();
        SequenceFileDirectoryReader pointsReader = null;
        try {
            IntWritable k = new IntWritable();
            WeightedVectorWritable wvw = new WeightedVectorWritable();
            pointsReader = new SequenceFileDirectoryReader(clusteredPointsPath);
            while (pointsReader.next(k, wvw)) {                
                clusteredPoints.add(new Pair<Integer,Vector>(k.get(), wvw.getVector()));
            }
        } catch (IOException e) {
            LOG.error("IOException caught while reading clustered points", e);
        } finally {
            if (pointsReader != null) {
                pointsReader.close();
            }
        }
        
        return clusteredPoints;
    }
    
    public List<Cluster> readClustersIteration(Path clusterIterationPath) {
        List<Cluster> clusters = new ArrayList<Cluster>();
        SequenceFileDirectoryReader iterationReader = null;
        try {
            Text k = new Text();
            Cluster c = new Cluster();
            iterationReader = new SequenceFileDirectoryReader(clusterIterationPath);
            while (iterationReader.next(k, c)) {
                clusters.add(c);
            }
        } catch (IOException e) {
            LOG.error("IOException caught while reading clustered points", e);
        } finally {
            if (iterationReader != null) {
                iterationReader.close();
            }
        }

        return clusters;
    }
    
}
