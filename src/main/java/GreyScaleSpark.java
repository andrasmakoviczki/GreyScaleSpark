import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bytedeco.javacpp.opencv_core;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Created by AMakoviczki on 2018. 05. 09..
 */
public class GreyScaleSpark {
    public static void main(String[] args) {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        SparkConf conf = new SparkConf().setAppName("GreyScale")
                //.setMaster("yarn-cluster")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text,BytesWritable> distFile = sc.sequenceFile(args[0], Text.class, BytesWritable.class);

        JavaPairRDD<Text,BufferedImage> matPair = distFile.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>, Text, BufferedImage>() {
            @Override
            public Tuple2<Text, BufferedImage> call(Tuple2<Text, BytesWritable> textByteWritableTuple2) throws Exception {
                InputStream in = new ByteArrayInputStream(textByteWritableTuple2._2().getBytes());
                BufferedImage bImageFromConvert = ImageIO.read(in);

                Mat mat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CvType.CV_8UC3);
                mat.put(0, 0, textByteWritableTuple2._2().getBytes());


                Mat mat1 = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), opencv_core.CV_8UC1);
                Imgproc.cvtColor(mat, mat1, Imgproc.COLOR_RGB2GRAY);

                byte[] data1 = new byte[mat1.rows() * mat1.cols() * (int) (mat1.elemSize())];
                mat1.get(0, 0, data1);
                BufferedImage image1 = new BufferedImage(mat1.cols(), mat1.rows(), BufferedImage.TYPE_BYTE_GRAY);
                image1.getRaster().setDataElements(0, 0, mat1.cols(), mat1.rows(), data1);

                Tuple2<Text, BufferedImage> tuple = new Tuple2<Text, BufferedImage>(textByteWritableTuple2._1(), image1);
                return tuple;
            }
        });

        System.out.println(matPair.count());
    }

}
