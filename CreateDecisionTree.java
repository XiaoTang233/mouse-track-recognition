import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import scala.Tuple2;

public class CreateDecisionTree implements Serializable{
	public static String saveModelPath = "hdfs://127.0.0.1:8020/RFmodel";
	
	public void createTree(JavaRDD<LabeledPoint> trainData , JavaSparkContext sc) throws IOException {		
		
		Configuration conf = new Configuration();
		FileSystem fs  = FileSystem.get(conf);		
		
		Map<Integer, Integer> categoricalFeatureInfo = new HashMap<>();
		double maxRight = 0;
		DecisionTreeModel currModel = null;
		int currDepth = 0;
		int currBins = 0;
		//int numTrees = 3;
		//int seed = 12345;
		String impurity = "gini";
		//String featureSubsetStrategy = "auto";
		int numClassess = 2;
		JavaRDD<LabeledPoint>[] s = trainData.randomSplit(new double[] {0.6,0.4});
		JavaRDD<LabeledPoint> train_data = s[0];
		JavaRDD<LabeledPoint> test_data = s[1];
		
		for(int maxDepth=3; maxDepth<10; maxDepth++) {
			for(int maxBins=30;maxBins<70;maxBins++) {
					DecisionTreeModel model = DecisionTree.trainClassifier(train_data, numClassess, categoricalFeatureInfo, impurity, maxDepth, maxBins);
					//RandomForestModel model = RandomForest.trainClassifier(train_data, numClassess, categoricalFeatureInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,seed);
					
					JavaPairRDD<Double, Double> predictAndReal = test_data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
						public Tuple2<Double, Double> call(LabeledPoint arg0) throws Exception {
							return new Tuple2<Double, Double>(model.predict(arg0.features()), arg0.label());
						}			
					});
					
					JavaPairRDD<Double, Double> predictrights = predictAndReal.filter(new Function<Tuple2<Double, Double>,Boolean>(){
						@Override
						public Boolean call(Tuple2<Double, Double> arg0) throws Exception {
							// TODO Auto-generated method stub
							if(arg0._1().equals(arg0._2()))
								return true;
							else
								return false;
						}
					});
					double rights = (double)predictrights.count()/test_data.count();
					if(rights > maxRight) {
							maxRight = rights;
							currModel = model;
							currDepth = maxDepth;
							currBins = maxBins;
					}
				
			}
		}
		if(fs.exists(new Path(saveModelPath))) {
			fs.delete(new Path(saveModelPath), true);
		}
		currModel.save(sc.sc(), saveModelPath);
		
		System.out.println("-------------------------");
		System.out.println("max Right number: "+maxRight);
		System.out.println("curr depth number: "+currDepth);
		System.out.println("curr bins number: "+currBins);
		System.out.println("-------------------------");
	}
}
