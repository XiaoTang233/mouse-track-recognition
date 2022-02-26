import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import scala.Tuple2;


public class main implements Serializable{
	public static String testTxtPath_5 = "hdfs://127.0.0.1:8020/ProjectData/txfz_test5.txt";
	public static String testTxtPath_7 = "hdfs://127.0.0.1:8020/ProjectData/txfz_test7.txt";
	public static String trainTxtPath = "hdfs://127.0.0.1:8020/ProjectData/txfz_training.txt";
	public static String trainFeaPath = "hdfs://127.0.0.1:8020/trainFeatureFile";
	public static String testFeaPath = "hdfs://127.0.0.1:8020/testFeatureFile";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SparkConf sparkconf = new SparkConf().setAppName("MouseTrajectoryJudge").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);
		ProcessData processdata = new ProcessData();
		
		
		
		//JavaRDD<LabeledPoint> trainData = processdata.getLabelPointData(trainTxtPath, trainFeaPath, sc);		
		//(new CreateDecisionTree()).createTree(trainData, sc);
		
		JavaRDD<LabeledPoint> testData1 = processdata.getLabelPointData(testTxtPath_5, testFeaPath, sc);
		JavaRDD<LabeledPoint> testData2 = processdata.getLabelPointData(testTxtPath_7, testFeaPath, sc);
		DecisionTreeModel rfm = DecisionTreeModel.load(sc.sc(), new CreateDecisionTree().saveModelPath);		
		processdata.printPredictResult(rfm, testData1);
		processdata.printPredictResult(rfm, testData2);
		
		sc.close();
	}
	
	
}
