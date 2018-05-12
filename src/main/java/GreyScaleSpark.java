import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import scala.Tuple2;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.opencv.core.CvType.CV_8UC1;
import static org.opencv.core.CvType.CV_8UC3;
import static org.opencv.imgproc.Imgproc.COLOR_RGB2GRAY;


/**
 * Created by AMakoviczki on 2018. 05. 09..
 */
public class GreyScaleSpark {
    class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat {
        @Override
        protected String generateFileNameForKeyValue(Object key, Object value, String name) {
            return key.toString();
        }

        @Override
        protected Object generateActualKey(Object key, Object value) {
            return NullWritable.get();
        }
    }

    public static void main(String[] args) throws IOException {
        //Loader.load(opencv_java.class);
        System.loadLibrary(org.opencv.core.Core.NATIVE_LIBRARY_NAME);

        SparkConf conf = new SparkConf().setAppName("GreyScale");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopConf = new Configuration();
        final String hdfsPath = args[1];
        conf.set("fs.defaultFS", hdfsPath);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp");

        final FileSystem fs = FileSystem.get(URI.create(hdfsPath), hadoopConf);
        Path outputPath = new Path(hdfsPath);


        JavaPairRDD<Text, BytesWritable> distFile = sc.sequenceFile(args[0], Text.class, BytesWritable.class);

        JavaPairRDD<Text, BufferedImage> matPair = distFile.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>, Text, BufferedImage>() {
            @Override
            public Tuple2<Text, BufferedImage> call(Tuple2<Text, BytesWritable> textByteWritableTuple2) throws Exception {

                BufferedImage bufimage = new BufferedImage(100, 100,
                        BufferedImage.TYPE_INT_ARGB);
                Text emptyImage = new Text("empty");

                try {
                    InputStream in = new ByteArrayInputStream(textByteWritableTuple2._2().getBytes());
                    BufferedImage bImageFromConvert = ImageIO.read(in);

                    if (bImageFromConvert != null) {
                        //System.out.println(textByteWritableTuple2._1().toString() + ": " + bImageFromConvert.getHeight() + " " + bImageFromConvert.getWidth());
                        Mat mat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC3);
                        mat.put(0,0,textByteWritableTuple2._2().getBytes());

                        Mat mat1 = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC1);
                        Imgproc.cvtColor(mat, mat1, COLOR_RGB2GRAY);

                        byte[] data1 = new byte[mat1.rows() * mat1.cols() * (int) (mat1.elemSize())];
                        mat1.get(0,0,data1);
                        BufferedImage image1 = new BufferedImage(mat1.cols(), mat1.rows(), BufferedImage.TYPE_BYTE_GRAY);
                        image1.getRaster().setDataElements(0, 0, mat1.cols(), mat1.rows(), data1);

                        Tuple2<Text, BufferedImage> tuple = new Tuple2<Text, BufferedImage>(new Text("grey-" + new File(textByteWritableTuple2._1().toString()).getName()), image1);

                        return tuple;
                    }
                } catch (UnsupportedOperationException ex) {

                } catch (NullPointerException ex) {

                } catch (IIOException ex) {

                } catch (IndexOutOfBoundsException ex) {

                }
                return new Tuple2<Text, BufferedImage>(emptyImage, bufimage);
            }
        });

        System.out.println(matPair.count());
    }

}
