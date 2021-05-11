import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex1 {

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("example")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("/home/bruno-santos/Desktop/G_v1GCD/datasets/title.basics.tsv.bz2");

        df.createOrReplaceTempView("title_basics");
        //df.printSchema();

        spark.sql("select startYear, count(startYear) " +
                "from title_basics " +
                "where titleType == 'movie' " +
                "group by startYear " +
                "order by startYear ASC;")
              .show();
    }
}

/*
root
 |-- tconst: string (nullable = true)
 |-- titleType: string (nullable = true)
 |-- primaryTitle: string (nullable = true)
 |-- originalTitle: string (nullable = true)
 |-- isAdult: string (nullable = true)
 |-- startYear: string (nullable = true)
 |-- endYear: string (nullable = true)
 |-- runtimeMinutes: string (nullable = true)
 |-- genres: string (nullable = true)

 */
