import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Ex3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //file title (idMovie, nameMovie)
        JavaPairRDD<String, String> leftJoin = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        //file rating (idMovie, rating)
        JavaPairRDD<String, String> rightJoin = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]));

        // inner join
        // {idMovie, {nameMovie, rating}}
        JavaPairRDD<String,String> innerrdd = leftJoin
                .join(rightJoin)
                .mapToPair(l -> new Tuple2<>(l._2._1,l._2._2));

        JavaRDD<String> print = innerrdd
                .map(l -> {
                    String nameMovie = l._1;
                    String rating = l._2;
                    return String.format("%s   ->   %s",nameMovie,rating);
                });

        print.saveAsTextFile("output/Ex3");




        /*

        for (Tuple2<String, Tuple2<String, String>> tuple : innerrdd.collect()) {
            System.out.println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2);
        }

        JavaRDD<String> rdd = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.basics.tsv.bz2");
        JavaRDD<String> rdd2 = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.ratings.tsv.bz2");

        JavaPairRDD<String,String> pairRdd = rdd
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        JavaPairRDD<String, String> pairRdd2 = rdd2
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]));

        JavaPairRDD<String, Tuple2<String, String>> joined = leftJoin.join(rightJoin);
        *
        *
        * */


    }
}
