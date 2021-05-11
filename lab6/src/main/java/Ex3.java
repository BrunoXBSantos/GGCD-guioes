import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class Ex3 {


    public static void main(String[] args) throws InterruptedException {
        // cada minuto top 3 dos ultimos 10 min
        // length window = 10m
        // slide window = 1m

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Lab6Ex3");

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/tmp/Ex3", () -> {
            JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

            JavaPairRDD<String,String> movies = sc.sparkContext().textFile("/home/bruno-santos/Desktop/G_v1GCD/datasets/title.basics.tsv.bz2")
                    .map(t -> t.split("\t"))
                    .filter(t -> !t[0].equals("tconst"))
                    .mapToPair(t -> new Tuple2<>(t[0], t[2]))
                    .cache();

            sc.socketTextStream("localhost", 12345)
                    .window(Durations.minutes(10),Durations.seconds(60))
                    .transformToPair(rdd -> {
                        JavaPairRDD<String, Double> res = rdd
                            .map(t -> t.split("\t"))
                            .mapToPair(t -> new Tuple2<>(Double.parseDouble(t[1]), t[0]))
                            .sortByKey(false)
                            .mapToPair(t -> new Tuple2<>(t._2,t._1))
                            .join(movies)
                            .mapToPair(t -> new Tuple2<>(t._2._2, t._2._1));
                        return res;
                    })
                    .foreachRDD(rdd -> {
                        List<Tuple2<String, Double>> lst = rdd.take(3);
                        System.out.println("TOP 3: " + lst.toString());
                    });
            sc.checkpoint("/tmp/Ex3");
            return sc;
        });

        jsc.start();
        jsc.awaitTermination();
    }
}
