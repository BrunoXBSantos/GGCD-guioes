package MapReduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Ex5 {


    // n_linha - linha - rating - name_filme
    public static class RatingLeast9Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineFields = value.toString().split("\t");
            Double rating = Double.parseDouble(lineFields[1]);
            if(rating >= 9.0)
                context.write(new DoubleWritable(rating),new Text(lineFields[0]));  // (rating, name_filme)
        }
    }

    // recebe os tipos de dados que vem do mapper
    // rating - name_filme - name_filme - rating
    public static class Ex5ReduceTask extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                context.write(value, new Text(key.toString()));  // name_fime - rating
            }
        }
    }



    // para fazer a comparacao de forma decrescente
    public static class sortComparator extends WritableComparator {

        protected sortComparator() {
            super(DoubleWritable.class, true);
            // TODO Auto-generated constructor stub
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            DoubleWritable k1 = (DoubleWritable) o1;
            DoubleWritable k2 = (DoubleWritable) o2;
            int cmp = k1.compareTo(k2);
            return -1 * cmp;
        }

    }

}
