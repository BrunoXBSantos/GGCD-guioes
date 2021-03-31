package MapReduce;

// tenho dois ficheiros grandes
// basics e o ratings

//    basics                        ratins
//  tconst  primaryTotle       tconst   averagerating
//
// vou fazer um shuffle join
//

import Utilis.PairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Ex4 {

    // n_linha, linha, identificador_filme, L,nome_filme
    public static class Ex4LeftMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text keyID = new Text();
            Text valName = new Text();
            if(key.get() == 0){
                String line = value.toString(); // Skip header
            }
            else{
                //System.out.println(key.get());
                String[] lineFields = value.toString().split("\t");
                String idMovie = lineFields[0];
                String nameMovie = lineFields[3];
                //PairWritable pair = new PairWritable("L", nameMovie);
                context.write(new Text(idMovie), new Text("L" + ",\t" + nameMovie));  // ("tt0886437", "L,    nameMovie")
            }
        }
    }

    // n_linha, linha, identificador_filme, nome filme
    public static class Ex4RightMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() == 0){
                String line = value.toString(); // Skip header
            }
            else{
                //System.out.println(key.get());
                String[] lineFields = value.toString().split("\t");
                String idMovie = lineFields[0];
                String ratingMovie = lineFields[1];
                //PairWritable pair = new PairWritable("R", ratingMovie);
                context.write(new Text(idMovie), new Text("R" + ",\t" + ratingMovie));   // ("tt0886437", "R,    ratingMovie")
            }
        }
    }

    // recebe os tipos de dados que vem do mapper
    //iD movie - (L, name) -
    //  ou
    //iD movie - (R, rating) -
    // no agrupamento e depois no reduce recebe isto
    // (iD movie, ((L, name), (R, rating)))
    public static class Ex4Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nameMovie;
            String ratingMovie;
            List<String> left = new ArrayList<>();
            List<String> right = new ArrayList<>();
            for(Text value: values){
                String[] temp = value.toString().split(",\t");
                if(temp[0].equals("L"))
                    left.add(temp[1]);
                else
                    right.add(temp[1]);
            }
            for(String name: left){
                nameMovie = name;
                for(String rating: right){
                    ratingMovie = rating;
                    context.write(new Text(nameMovie), new Text(ratingMovie));
                }
            }
        }
    }

}
