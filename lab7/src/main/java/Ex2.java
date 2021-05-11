import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex2 {

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("lab7Ex2")
                .master("local")
                .getOrCreate();

        // dataframe do csv title.basics
        // contem o code, name e year do movie
        Dataset<Row> df_movieYear = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("/home/bruno-santos/Desktop/G_v1GCD/datasets/title.basics.tsv.bz2");

        // dataframe do csv title.rating
        // contem o code do movie e o rating
        Dataset<Row> df_movieRating = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("/home/bruno-santos/Desktop/G_v1GCD/datasets/title.ratings.tsv.bz2");

        df_movieYear.createOrReplaceTempView("title_basics");
        df_movieYear.printSchema();

        df_movieRating.createOrReplaceTempView("title_ratings");
        df_movieRating.printSchema();


        spark.sql("SELECT first(tb.primaryTitle), tb.startYear, MAX(tr.averageRating) " +
                "FROM title_basics AS tb " +
                "INNER JOIN title_ratings AS tr " +
                "ON tb.tconst == tr.tconst " +
                "WHERE tb.titleType == 'movie' " +
                "GROUP BY tb.startYear " +
                "ORDER BY tb.startYear ASC NULLS FIRST")
                .write()
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .csv("resultEx2-csv");

    }
}

/*   title_basics
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

/*  title_ratings
root
 |-- tconst: string (nullable = true)
 |-- averageRating: string (nullable = true)
 |-- numVotes: string (nullable = true)
 */
