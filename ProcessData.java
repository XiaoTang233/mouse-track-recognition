import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;


public class ProcessData implements Serializable{
	/*get data (id,label,x[],y[],time[])*/
	public JavaPairRDD<Integer, Tuple2<Integer, List<Vector<Integer>>>> createTuple(JavaRDD<String> inData){
		
		JavaPairRDD<Integer, Tuple2<Integer, List<Vector<Integer>>>> outData = inData.mapToPair(new PairFunction<String, Integer, Tuple2<Integer, List<Vector<Integer>>>>() {

			@Override
			public Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				int label;
				String[] s1 = line.split(" ");
				int id = Integer.parseInt(s1[0]);
				if(s1.length == 4)
					label = Integer.parseInt(s1[3]);
				else
					label = 0;
				String[] s2 = s1[1].split(";");
				int tmpTime = -1;
				Vector<Integer> px = new Vector<Integer>();
				Vector<Integer> py = new Vector<Integer>();
				Vector<Integer> time = new Vector<Integer>();
				for(int i=0;i<s2.length;i++) {
					String[] s3 = s2[i].split(",");
					if(Integer.parseInt(s3[2]) == tmpTime)
						continue;
					tmpTime = Integer.parseInt(s3[2]);
					px.add(Integer.parseInt(s3[0]));
					py.add(Integer.parseInt(s3[1]));
					time.add(Integer.parseInt(s3[2]));
				}

				List<Vector<Integer>> list = new ArrayList<Vector<Integer>>();
				list.add(px);
				list.add(py);
				list.add(time);
				
				return new Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>>(id, new Tuple2<Integer, List<Vector<Integer>>>(label, list));
			}
			
		});
		
		outData = outData.filter(new Function<Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>> line) throws Exception {
				// TODO Auto-generated method stub
				List<Vector<Integer>> list = line._2()._2();
				Vector<Integer> px = list.get(0);
				int len = px.size();
				if(len<=2)
					return false;
				return true;
			}
		});
		
		return outData;
	}
	
	/*change format and save as csv*/
	public void transFormatAndSave(JavaPairRDD<Tuple2<Integer, Integer>, Vector<Double>> featureData, String outPath) throws IllegalArgumentException, IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs  = FileSystem.get(conf);
		
		int featureNum = (new CatchFeature()).featureNum;
		JavaRDD<String> message = featureData.map(new Function<Tuple2<Tuple2<Integer, Integer>, Vector<Double>>, String>() {
			@Override
			public String call(Tuple2<Tuple2<Integer, Integer>, Vector<Double>> line) throws Exception {
				// TODO Auto-generated method stub
				int label = line._1()._2();
				Vector<String> v = new Vector<String>();
				for(int i=1;i<=featureNum;i++) {
					String s = " "+i+":"+line._2().elementAt(i-1);
					v.add(s);
				}
				String str = ""+label;
				for(int t=0;t<v.size();t++) {
					str = str+v.elementAt(t);
				}
				return str;
			}		
		});
		if(fs.exists(new Path(outPath))) {
			fs.delete(new Path(outPath), true);
		}
		
		message.saveAsTextFile(outPath);
	}
	
	public JavaRDD<LabeledPoint> getLabelPointData(String inpath, String outPath, JavaSparkContext sc) throws IllegalArgumentException, IOException{
		CatchFeature catchfeature = new CatchFeature();
		JavaRDD<String> trainData = sc.textFile(inpath);
		JavaPairRDD<Integer, Tuple2<Integer, List<Vector<Integer>>>> trainDataTuple = createTuple(trainData);
		JavaPairRDD<Tuple2<Integer,Integer>,Vector<Double>> featureData = catchfeature.runCatchFeature(trainDataTuple);
		transFormatAndSave(featureData, outPath);
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), outPath).toJavaRDD();
		
		return data;
	}

	public void printPredictResult(DecisionTreeModel model, JavaRDD<LabeledPoint> testData) throws IOException {
		String resultSavePath = "hdfs://127.0.0.1:8020/results";
		Configuration conf = new Configuration();
		FileSystem fs  = FileSystem.get(conf);
		
		JavaRDD<String> results = testData.map(new Function<LabeledPoint,String>() {
			@Override
			public String call(LabeledPoint line) throws Exception {
				// TODO Auto-generated method stub
				double p = model.predict(line.features());
				return ""+p;
			}
		});
		List<String> list = results.collect();
		Vector<String> str = new Vector<String>();
		for(int i=0;i<list.size();i++) 
			str.add((i+1)+": "+list.get(i));
		System.out.println("---------------results-----------------");
		for(int t=0;t<list.size();t++) {
			System.out.println(str.elementAt(t));
		}
		System.out.println("---------------------------------------");
	}
}
