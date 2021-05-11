import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class Ex1 {

    // num de filmes para cada ATOR
    // ver fix title.principals.tsv que tem para cada filmes os seus participantes
    // dai tiramos apenas os atores

    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";


    public static void main(String[] args) {

        // num de filmes para cada ator
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab5spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);

        // codigo e nome dos atores (codPessoa - nomePessoa)
        //                          (nconst   -  primaryName)
        JavaPairRDD<String, String> rddNames = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/name.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("nconst"))
                .mapToPair(l -> new Tuple2<>(l[0],l[1]));

        // codigo ator - num filmes participou
        //(nconst   -  1)
        JavaPairRDD<String, Integer> rddNumMovies = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.principals.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> (l[3].toUpperCase(Locale.ROOT).equals("ATOR") || l[3].toUpperCase(Locale.ROOT).equals("ACTRESS")))
                .mapToPair(l -> new Tuple2<>(l[2],1))
                .foldByKey(0, Integer::sum);

        // join para ter o nome e o num de movies que participou ordenado por num filme que participou
        JavaPairRDD<String, Integer> innerJoin = rddNames
                .join(rddNumMovies)
                //(nconst, (primaryName, numMovies))
                .mapToPair(l -> new Tuple2<>(l._2._2,l._2._1))
                .sortByKey(false)
                .mapToPair(l -> new Tuple2<>(l._2,l._1))
                .cache();


        innerJoin.saveAsTextFile("output/Ex1"); // parte 1
        //List<Tuple2<String, Integer>> actors = innerJoin.collect();
        List<Tuple2<String, Integer>> top10 = innerJoin.take(10);  // parte 2

        System.out.println("\t10 atores/as com mais filmes: \n");
        for(Tuple2<String, Integer> t: top10){
            System.out.println(ANSI_GREEN + t._1 + "\t->\t" + t._2 + ANSI_RESET);
        }
        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }

}
