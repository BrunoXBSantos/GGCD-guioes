package Main;

import MapReduce.Ex1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Ex1_Main {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        try {

            // config uma tarefa
            Job job = Job.getInstance(new Configuration(), "Ex1");

            // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
            job.setJarByClass(Ex1.class);

            // configurar cada uma das classes: map e reduce
            job.setMapperClass(Ex1.Ex1Mapper.class);
            job.setReducerClass(Ex1.Ex1Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.basics.tsv"));

            //config o fix de saida
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path("Ex1_output"));

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //config os tipos de dados comunicados do mapa para o reduce
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // executar a tarefa
            job.waitForCompletion(true);


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
