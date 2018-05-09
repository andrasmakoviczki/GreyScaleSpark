import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by AMakoviczki on 2018. 05. 09..
 */
public class GreyScaleSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GreyScale").setMaster("");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text,ByteWritable> distFile = sc.sequenceFile(args[0], Text.class, ByteWritable.class);

        System.out.println(distFile.count());
    }
}
