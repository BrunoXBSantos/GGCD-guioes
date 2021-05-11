import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;
import java.util.stream.StreamSupport;

public class Ex2 {

    public static void main(String[] args) throws InterruptedException {
        // cada minuto top 3 dos ultimos 10 min
        // length window = 10m
        // slide window = 1m

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Lab6Ex2");

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/tmp/stream", () -> {
            JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
            sc.socketTextStream("localhost", 12345)
                .window(Durations.minutes(10),Durations.seconds(15))
                .transform(rdd ->
                        rdd.map(t -> t.split("\t"))
                )
                .transformToPair(rdd ->
                        rdd.mapToPair(t -> new Tuple2<>(Double.parseDouble(t[1]), t[0]))
                           .sortByKey(false)
                )
                .foreachRDD(rdd -> {
                    List<Tuple2<Double, String>> lst = rdd.take(3);
                    System.out.println("TOP 3: " + lst.toString());
                });
            sc.checkpoint("/tmp/stream");
            return sc;
        });

        jsc.start();
        jsc.awaitTermination();
// 41, 62, 62
    }
}

/*
    JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
        sc.socketTextStream("localhost", 12345)
                .window(Durations.minutes(10),Durations.seconds(60))
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],Double.parseDouble(t[1])))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(
                        StreamSupport.stream(p._2.spliterator(),false)
                                .mapToDouble(a -> a)
                                .average()
                                .getAsDouble(), p._1
                ))
                .foreachRDD(rdd -> {
                    List<Tuple2<Double, String>> lst = rdd.sortByKey(false).take(3);
                    System.out.println("TOP 3: " + lst.toString());
                });

        sc.start();
        sc.awaitTermination();
 */
