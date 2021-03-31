package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MapReduce {
    // n_linha, linha, genero, numero(1)
    public static class Ex1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() == 0){
                String line = value.toString(); // Skip header
            }
            else{
                //System.out.println(key.get());
                String[] lineFields = value.toString().split("\t");
                String[] genres = lineFields[8].split(",");
                for(String genre: genres){
                    context.write(new Text(genre.toLowerCase()), new LongWritable(1));
                }
            }
        }
    }


    // recebe os tipos de dados que vem do mapper
    //e produz o que quisermos: um ficheiro. A chave é uma palavra e o valor uma contagem formatada como texto
    public static class Ex1Reduce extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value: values){
                total += value.get();
            }
            context.write(key, new Text(Long.toString(total)));
        }
    }

}
