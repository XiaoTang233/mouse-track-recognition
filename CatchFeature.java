import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CatchFeature implements Serializable{
	public static int featureNum = 22;
	public JavaPairRDD<Tuple2<Integer, Integer>, Vector<Double>> runCatchFeature(JavaPairRDD<Integer, Tuple2<Integer, List<Vector<Integer>>>> trainDataTuple) {

		/*(<id, label>, <feature[], value[]>)*/
		JavaPairRDD<Tuple2<Integer,Integer>, Vector<Double>> featurePair = 
				trainDataTuple.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>>, Tuple2<Integer,Integer>,Vector<Double>>() {
					
					@Override
					public Tuple2<Tuple2<Integer, Integer>, Vector<Double>> call(
							Tuple2<Integer, Tuple2<Integer, List<Vector<Integer>>>> line) throws Exception {
						// TODO Auto-generated method stub
						int id = line._1();
						int label = line._2()._1();
						Vector<Double> featureValue = new Vector<Double>();
						
						
						featureValue.add(averSpeed(line._2()._2()));
						featureValue.add(averAccelSpeed(line._2()._2()));
						featureValue.add(maxSpeed(line._2()._2()));
						featureValue.add(minSpeed(line._2()._2()));
						featureValue.add(MaxInterval(line._2()._2().get(0)));
						featureValue.add(MaxInterval(line._2()._2().get(1)));
						featureValue.add(maxXorY(line._2()._2().get(0)));
						featureValue.add(maxXorY(line._2()._2().get(1)));
						featureValue.add(sampleTime(line._2()._2().get(0)));
						featureValue.add(backTimes(line._2()._2().get(0)));
						featureValue.add(useTime(line._2()._2().get(2)));
						featureValue.add(averAngle(line._2()._2()));
						featureValue.add(repetitiveXY(line._2()._2().get(0)));
						featureValue.add(repetitiveXY(line._2()._2().get(1)));
						featureValue.add(repetitivePoints(line._2()._2()));
						featureValue.add(distance(line._2()._2()));
						featureValue.add(straightLength(line._2()._2()));
						featureValue.add(routeRatio(line._2()._2()));
						featureValue.add(averTimeInterval(line._2()._2().get(2)));
						featureValue.add(shift(line._2()._2().get(0)));
						featureValue.add(shift(line._2()._2().get(1)));
						featureValue.add(speedVariance(line._2()._2()));
						
						

						Tuple2<Integer, Integer> idLabel = new Tuple2<Integer, Integer>(id, label);
						return new Tuple2<Tuple2<Integer, Integer>, Vector<Double>>(idLabel, featureValue);
					}
		});
		
		return featurePair;
	}
	
	/*feature 1: speed*/
	public double averSpeed(List<Vector<Integer>> list) {
		try {
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			Vector<Integer> time = list.get(2);
			int len = px.size();

			double distence = 0;
			for(int i=0;i<len-1;i++) {
				int x0 = px.elementAt(i);
				int x1 = px.elementAt(i+1);
				int y0 = py.elementAt(i);
				int y1 = py.elementAt(i+1);
				distence = distence + Math.sqrt(Math.pow(x0-x1, 2) + Math.pow(y0-y1, 2));
			}
			int allTime = time.elementAt(len-1) - time.elementAt(0);
			double speed = distence / allTime;	
			return speed;
		}catch(Exception e) {
			e.printStackTrace();
			return -9999;
		}
	}
	
	public double speed(int x0, int x1, int y0, int y1, int time0, int time1) {
		
		return Math.sqrt(Math.pow(x0-x1, 2) + Math.pow(y0-y1, 2)) / (time1 - time0);
	}
	
	/*feature 2: accelerated speed*/
	public double averAccelSpeed(List<Vector<Integer>> list) {
		try {
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			Vector<Integer> time = list.get(2);
			int len = px.size();

			double v0 = speed(px.elementAt(0),px.elementAt(1),py.elementAt(0),py.elementAt(1),time.elementAt(0),time.elementAt(1));
			double vlast = speed(px.elementAt(len-2),px.elementAt(len-1),py.elementAt(len-2),py.elementAt(len-1),time.elementAt(len-2),time.elementAt(len-1));
			double aas = (vlast-v0) / (time.elementAt(len-1) - time.elementAt(0));
			
			return aas;
			
		}catch(Exception e) {
			e.printStackTrace();
			return -9999;
		}
	}
	
	/*feature 3: max speed*/
	public double maxSpeed(List<Vector<Integer>> list) {
		try {
			double max = 0.0;
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			Vector<Integer> time = list.get(2);
			int len = px.size();
			
			for(int i=0;i<len-1;i++) {
				double speed = speed(px.elementAt(i),px.elementAt(i+1),py.elementAt(i),py.elementAt(i+1),time.elementAt(i),time.elementAt(i+1));
				if(speed > max)
					max = speed;
			}
			return max;
			
		}catch(Exception e){
			e.printStackTrace();
			return -9999;
		}
	}
	
	/*feature 4: min speed*/
	public double minSpeed(List<Vector<Integer>> list) {
		try {
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			Vector<Integer> time = list.get(2);
			int len = px.size();
			double min = speed(px.elementAt(0),px.elementAt(1),py.elementAt(0),py.elementAt(1),time.elementAt(0),time.elementAt(1));
			
			for(int i=1;i<len-1;i++) {
				double speed = speed(px.elementAt(i),px.elementAt(i+1),py.elementAt(i),py.elementAt(i+1),time.elementAt(i),time.elementAt(i+1));
				if(speed < min)
					min = speed;
			}
			return min;
			
		}catch(Exception e) {
			e.printStackTrace();
			return -9999;
		}
	}
	
	/*feature 5,6: max interval of X or Y*/
	public double MaxInterval(Vector<Integer> px) {
		try {
			int len = px.size();
			double max = 0.0;
			for(int i=0;i<len-1;i++) {
				double interval = Math.pow(px.elementAt(i)-px.elementAt(i+1), 2);
				if(interval > max)
					max = interval;
			}			
			return Math.sqrt(max);
		}catch(Exception e) {
			e.printStackTrace();
			return -9999;
		}
	}
	
	/*feature 7,8: max X or Y*/
	public double maxXorY(Vector<Integer> p) {
		try {
			double max = 0.0;
			int len = p.size();
			for(int i=0;i<len;i++) {
				if(p.elementAt(i) > max)
					max = p.elementAt(i);
			}
			return max;
			
		}catch(Exception e) {
			e.printStackTrace();
			return -9999;
		}
	}
	
	/*feature 9: sampling times*/
	public double sampleTime(Vector<Integer> p) {
		return p.size();
	}
		
	/*feature 10: mouse back times, which means x(i+1) - x(i) is a negative number*/
	public double backTimes(Vector<Integer> px) {
		try {
			double count = 0.0;
			int len = px.size();
			for(int i=0;i<len-1;i++) {
				if(px.elementAt(i+1)-px.elementAt(i) < 0)
					count++;
			}
			return count;
			
		}catch(Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	/*feature 11: use time*/
	public double useTime(Vector<Integer> time) {
		int len = time.size();
		return (time.elementAt(len-1) - time.elementAt(0));
	}
	
	
	
	public double getAngel(int point1, int point2, int point3, Vector<Integer> px,Vector<Integer> py) {
		double pi = Math.PI;
		int x0 = px.elementAt(point1);
		int x1 = px.elementAt(point2);
		int x2 = px.elementAt(point3);
		int y0 = py.elementAt(point1);
		int y1 = py.elementAt(point2);
		int y2 = py.elementAt(point3);
		
		if(((x2-x1)*(y2-y0)) == ((y2-y1)*(x2-x0)))
			return pi;
		
		double l1 = Math.sqrt(Math.pow((x0-x1),2) + Math.pow((y0-y1),2));
		double l2 = Math.sqrt(Math.pow((x1-x2),2) + Math.pow((y1-y2),2));
		double l3 = Math.sqrt(Math.pow((x2-x0),2) + Math.pow((y2-y0),2));
		
		if(l3==0 && l1!=0 && l2!=0)
			return 0;

		return Math.acos((Math.pow(l1, 2) + Math.pow(l2, 2) - Math.pow(l3, 2)) / (2*l1*l2));
	}
	
	/*feature 12: average angle*/
	public double averAngle(List<Vector<Integer>> list) {
		double pi = Math.PI;
		double angel = 0.0;
		Vector<Integer> px = list.get(0);
		Vector<Integer> py = list.get(1);
		Vector<Integer> newx = new Vector<Integer>();
		Vector<Integer> newy = new Vector<Integer>();
		int len = px.size();
		try {
			int tmpx = px.elementAt(0);
			int tmpy = py.elementAt(0);
			newx.add(tmpx);
			newy.add(tmpy);
			for(int i=1;i<len;i++) {
				if(tmpx == px.elementAt(i) && tmpy == py.elementAt(i)) 
					continue;
				tmpx = px.elementAt(i);
				tmpy = py.elementAt(i);
				newx.add(tmpx);
				newy.add(tmpy);
			}
			int newlen = newx.size();
			if(newlen==2)
				return pi;
			if(newlen<2)
				return 0;
			for(int j=0;j<newlen-2;j++) {
				double tmpAngel = getAngel(j,j+1,j+2,px,py);
				angel = angel + tmpAngel;
			}
			return angel / newlen;
		}catch(Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	/*feature 13 14: repetitive X or Y times*/
	public double repetitiveXY(Vector<Integer> p) {
		try {
			int len = p.size();
			double count = 0.0;
			for(int i=0;i<len-1;i++) {
				int x0 = p.elementAt(i);
				int x1 = p.elementAt(i+1);
				if(x0 == x1) 
					count++;
			}		
			return count;
		}catch(Exception e) {
			e.printStackTrace();
			return 0.0;
		}
	}
	
	/*feature 15: repetitive points*/
	public double repetitivePoints(List<Vector<Integer>> list) {
		try {
			double count = 0.0;
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			int len = px.size();
			for(int i=0; i<len-1; i++) {
				int x0 = px.elementAt(i);
				int x1 = px.elementAt(i+1);
				int y0 = py.elementAt(i);
				int y1 = py.elementAt(i+1);
				if(x0==x1 && y0==y1) 
					count++;		
			}			
			return count;
		}catch(Exception e) {
			e.printStackTrace();
			return -999;
		}
	}
	
	/*feature 16: distance*/
	public double distance(List<Vector<Integer>> list) {
		try {
			double distance = 0.0;
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			int len = px.size();
			
			for(int i=0;i<len-1;i++) {
				int x0 = px.elementAt(i);
				int x1 = px.elementAt(i+1);
				int y0 = py.elementAt(i);
				int y1 = py.elementAt(i+1);
				distance = distance + Math.sqrt(Math.pow(x0-x1, 2) + Math.pow(y0-y1, 2));
			}
			return distance;
		}catch(Exception e){
			e.printStackTrace();
			return -999;
		}
	}
	
	/*feature 17: straight length*/
	public double straightLength(List<Vector<Integer>> list) {
		try {
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			int len = px.size();
			
			int x0 = px.elementAt(0);
			int y0 = py.elementAt(0);
			int xlast = px.elementAt(len-1);
			int ylast = py.elementAt(len-1);		
			
			return Math.sqrt(Math.pow(xlast-x0, 2) + Math.pow(ylast-y0, 2));
		}catch(Exception e) {
			e.printStackTrace();
			return 0.0;
		}
	}
	
	/*feature 18: straight length/distance length*/
	public double routeRatio(List<Vector<Integer>> list) {
		try {
			double distance = distance(list);
			double straight = straightLength(list);
			if(distance==0)
				return 0;
			
			return straight/distance;			
		}catch(Exception e) {
			e.printStackTrace();
			return 0.0;
		}
	}
	
	/*feature 19: average time interval*/
	public double averTimeInterval(Vector<Integer> time) {
		try {
			int len = time.size();
			double allTime = useTime(time);
			return allTime/len;
			
		}catch(Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	/*feature 20 21: vertical or horizontal shift*/
	public double shift(Vector<Integer> p) {
		try {
			int len = p.size();
			double shift = p.elementAt(len-1) - p.elementAt(0);
			
			return shift;
		}catch(Exception e) {
			e.printStackTrace();
			return 0.0;
		}
	}
	
	/*feature 22: variance of speed*/
	public double speedVariance(List<Vector<Integer>> list) {
		try {
			Vector<Integer> px = list.get(0);
			Vector<Integer> py = list.get(1);
			Vector<Integer> time = list.get(2);
			int len = px.size();
			double aver = averSpeed(list);
			double addAll = 0.0;
			for(int i=0;i<len-1;i++) {
				double sp = speed(px.elementAt(i),px.elementAt(i+1),py.elementAt(i),py.elementAt(i+1),time.elementAt(i),time.elementAt(i+1));
				addAll = addAll + Math.pow(sp-aver, 2);
			}
				
			return addAll/(len-1);
		}catch(Exception e) {
			e.printStackTrace();
			return 0.0;
		}
	}
	
}
