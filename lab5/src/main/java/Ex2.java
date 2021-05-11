import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Ex2 {

    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) {

        // num de filmes para cada ator
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab5-Ex2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        // codigo filme - rating filme
        // (tconst   -  averageRating)
        JavaPairRDD<String, Float> movie_averageRating = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[1].equals("averageRating"))
                .mapToPair(l -> new Tuple2<>(l[0],Float.parseFloat(l[1])));

        // codigo filme - codigo ator
        //(tconst   -  nconst)
        JavaPairRDD<String, String> movie_actor = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.principals.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].toUpperCase(Locale.ROOT).equals("ACTOR") || l[3].toUpperCase(Locale.ROOT).equals("ACTRESS")  || l[3].toUpperCase(Locale.ROOT).equals("SELF"))
                .mapToPair(l -> new Tuple2<>(l[0],l[2]));

        // para o join fica:
        //                  (movie, (ator, rating))
        // resultado final:
        //                  Ator, List(movie,rating)
        JavaPairRDD<String, ArrayList<Tuple2<String, Float>>> ator_3movies = movie_actor
                .join(movie_averageRating)    // (movie, (ator, rating))
                .mapToPair(p -> new Tuple2<>(p._2._1, new Tuple2<>(p._1,p._2._2)))// (ator, (movie, rating))
                .groupByKey()  //(ator, (movie, rating) (movie, rating))
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .sorted((a, b) -> b._2.compareTo(a._2))
                                .collect(Collectors.toList())
                                //.subList(0, Math.min(size, 3))
                ))
                .mapToPair(p -> new Tuple2<>(p._1,
                        new ArrayList<>(p._2.subList(0, Math.min(p._2.size(),3)))));


        List<Tuple2<String, ArrayList<Tuple2<String, Float>>>> result = ator_3movies.collect();

        //ator_3movies.saveAsTextFile("output/Ex2"); // parte 1
        for(Tuple2<String, ArrayList<Tuple2<String, Float>>> r: result)
            System.out.println(ANSI_GREEN + r.toString() + ANSI_RESET);

        //for(int i = 0; i < result.size(); i++)
        //    System.out.println(ANSI_GREEN + result.get(i).toString() +  ANSI_RESET);

        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }

}
