import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class EX4 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //file title (idMovie, nameMovie)
        JavaPairRDD<String, String> leftJoin = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        //file rating (idMovie, rating)
        JavaPairRDD<String, Double> rightJoin = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // inner join
        JavaRDD<String> ex4 = leftJoin
                .join(rightJoin)
                .filter(l -> l._2._2 >= 9.0)
                .mapToPair(l -> new Tuple2<>(l._2._2,l._2._1))
                .sortByKey(false)
                .map(l -> {
                    String nameMovie = l._2;
                    Double rating = l._1;
                    return String.format("%s   ->   %.2f",nameMovie,rating);
                });

        ex4.saveAsTextFile("output/Ex4");
    }
}
