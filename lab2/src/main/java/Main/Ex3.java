package Main;

import MapReduce.MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Ex3 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        // config uma tarefa
        Job job = Job.getInstance(new Configuration(), "Ex3");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job.setJarByClass(MapReduce.class);

        // configurar cada uma das classes: map e reduce
        job.setMapperClass(MapReduce.Ex1Mapper.class);
        job.setReducerClass(MapReduce.Ex1Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs:///title.basics.tsv.gz"));

        //config o fix de saida
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs:///Ex3_output"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //config os tipos de dados comunicados do mapa para o reduce
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // executar a tarefa
        job.waitForCompletion(true);

    }
}
