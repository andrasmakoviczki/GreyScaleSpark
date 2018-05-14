package hu.elte.bigdata.thesis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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

    public static void main(String[] args) throws IOException {
        //Load the native library for OpenCV
        System.loadLibrary(org.opencv.core.Core.NATIVE_LIBRARY_NAME);

        //Handle arguments
        final String inputPath = args[0];
        final String outputPath = args[1];
        final String mode = args[2];

        //Set the default configurations
        SparkConf conf = new SparkConf().setAppName("GreyScaleSpark");
        Configuration hconf = HBaseConfiguration.create();

        hconf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        hconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

        Configuration hadoopConf = new Configuration();
        conf.set("fs.defaultFS", outputPath);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp");

        //Initialize Spark contexes
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaHBaseContext hc = new JavaHBaseContext(sc, hconf);

        //Create HDFS target directory
        final FileSystem fs = FileSystem.get(URI.create(outputPath), hadoopConf);
        Path output = new Path(outputPath);


        if (mode.equals("seqfile") || mode.equals("file")) {
            //Read the input
            JavaPairRDD<Text, BytesWritable> distFile = null;

            if (mode.equals("seqfile")) {
                distFile = sc.sequenceFile(inputPath, Text.class, BytesWritable.class);
            } else {
                distFile = sc.sequenceFile(inputPath + "/*", Text.class, BytesWritable.class);
            }

            //Implement mapper
            JavaPairRDD<Text, BufferedImage> matPair = distFile.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>, Text, BufferedImage>() {

                @Override
                public Tuple2<Text, BufferedImage> call(Tuple2<Text, BytesWritable> textByteWritableTuple2) throws Exception {

                    //Create empty image to handle the unsupported image format
                    BufferedImage bufimage = new BufferedImage(100, 100, BufferedImage.TYPE_BYTE_GRAY);
                    Text emptyImage = new Text("empty");

                    try {
                        //Read file to inputstream
                        InputStream in = new ByteArrayInputStream(textByteWritableTuple2._2().getBytes());
                        BufferedImage bImageFromConvert = ImageIO.read(in);

                        if (bImageFromConvert != null) {
                            //Get the image file
                            Mat mat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC3);
                            mat.put(0, 0, textByteWritableTuple2._2().getBytes());

                            Mat mat1 = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC1);
                            Imgproc.cvtColor(mat, mat1, COLOR_RGB2GRAY);

                            //Init new greyscale image
                            byte[] data1 = new byte[mat1.rows() * mat1.cols() * (int) (mat1.elemSize())];
                            mat1.get(0, 0, data1);

                            //Copy the image into greyscale image
                            BufferedImage image1 = new BufferedImage(mat1.cols(), mat1.rows(), BufferedImage.TYPE_BYTE_GRAY);
                            image1.getRaster().setDataElements(0, 0, mat1.cols(), mat1.rows(), data1);

                            //Pass the greyscale images to the new RDD
                            return new Tuple2<Text, BufferedImage>(new Text("grey-" + new File(textByteWritableTuple2._1().toString()).getName()), image1);
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
        } else if (mode.equals("hbase")) {

            //Create new HBase connection
            Connection connection = ConnectionFactory.createConnection(hconf);
            Admin admin = connection.getAdmin();

            //Set the table
            TableName tableName = TableName.valueOf(inputPath);
            Scan scan = new Scan();
            scan.setCaching(100);

            final JavaRDD<Tuple2<ImmutableBytesWritable, Result>> distFile = hc.hbaseRDD(tableName, scan);

            //Implement Mapper
            JavaRDD<Tuple2<Text, BufferedImage>> matRDD = distFile.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<Text, BufferedImage>>() {

                @Override
                public Tuple2<Text, BufferedImage> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {

                    //Create empty image to handle the unsupported image format
                    BufferedImage bufimage = new BufferedImage(100, 100, BufferedImage.TYPE_BYTE_GRAY);
                    Text emptyImage = new Text("empty");

                    try {
                        //Read file to inputstream
                        InputStream in = new ByteArrayInputStream(immutableBytesWritableResultTuple2._2().getColumnCells(Bytes.toBytes("image"), Bytes.toBytes("img")).get(0).getValueArray());
                        BufferedImage bImageFromConvert = ImageIO.read(in);

                        if (bImageFromConvert != null) {
                            Mat mat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC3);
                            mat.put(0, 0, immutableBytesWritableResultTuple2._2().getColumnCells(Bytes.toBytes("image"), Bytes.toBytes("img")).get(0).getValueArray());

                            Mat greyMat = new Mat(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), CV_8UC1);
                            Imgproc.cvtColor(mat, greyMat, COLOR_RGB2GRAY);

                            //Init new greyscale image
                            byte[] greyData = new byte[greyMat.rows() * greyMat.cols() * (int) (greyMat.elemSize())];
                            greyMat.get(0, 0, greyData);

                            //Copy the image into greyscale image
                            BufferedImage greyImage = new BufferedImage(greyMat.cols(), greyMat.rows(), BufferedImage.TYPE_BYTE_GRAY);
                            greyImage.getRaster().setDataElements(0, 0, greyMat.cols(), greyMat.rows(), greyData);

                            //Pass the greyscale images to the new RDD
                            return new Tuple2<Text, BufferedImage>(new Text("grey-" + new File(immutableBytesWritableResultTuple2._1().toString()).getName()), greyImage);
                        }
                    } catch (UnsupportedOperationException ex) {

                    } catch (NullPointerException ex) {

                    } catch (IIOException ex) {

                    } catch (IndexOutOfBoundsException ex) {

                    }
                    return new Tuple2<Text, BufferedImage>(emptyImage, bufimage);
                }
            });
            System.out.println(matRDD.count());
        } else {
            throw new UnsupportedOperationException("Unsupported mode");
        }

        sc.close();
    }

}
