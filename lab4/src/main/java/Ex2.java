import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Ex2 {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> mr = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .map(l -> l[8])
                .filter(l -> !l.equals("\\N"))
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                .mapToPair(l -> new Tuple2<>(l, 1))
                .foldByKey(0, (v1, v2) -> v1 + v2)
                .sortByKey();

        long initialTimeBzip = System.currentTimeMillis();
        mr.saveAsTextFile("output/Ex2_Bzip");
        long finalTimeBzip = System.currentTimeMillis();
        long dif = (finalTimeBzip - initialTimeBzip);
        String timeBzip = String.format("Tempo de execução Bzip: %02d segundos  e %02d milisegundos", dif/1000, dif%1000);


        JavaPairRDD<String, Integer> mr1 = sc.textFile("/home/bruno-santos/Desktop/GGCD/datasets/title.basics.tsv.gz")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .map(l -> l[8])
                .filter(l -> !l.equals("\\N"))
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                .mapToPair(l -> new Tuple2<>(l, 1))
                .foldByKey(0, (v1, v2) -> v1 + v2)
                .sortByKey();


        long initialTimeGz = System.currentTimeMillis();
        mr1.saveAsTextFile("output/Ex2_GZ");
        long finalTimeGz = System.currentTimeMillis();
        long difGz = (finalTimeGz - initialTimeGz);
        String timeGz = String.format("Tempo de execução Gz: %02d segundos  e %02d milisegundos", difGz/1000, difGz%1000);


        System.out.println(timeGz);
        System.out.println(timeBzip);

        /*
          Tempo de execução Gz: 09 segundos  e 846 milisegundos
          Tempo de execução Bzip: 00 segundos  e 278 milisegundos
        */
    }

}
