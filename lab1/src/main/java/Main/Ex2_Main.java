package Main;

import MapReduce.Ex2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Ex2_Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // config uma tarefa
        Job job = Job.getInstance(new Configuration(), "Ex2");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job.setJarByClass(Ex2.class);

        // configurar cada uma das classes: map e reduce
        job.setMapperClass(Ex2.Ex2Mapper.class);
        job.setCombinerClass(Ex2.Ex2Combine.class);  // o combiner é igual ao reducer neste caso
        job.setReducerClass(Ex2.Ex2Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.basics.tsv"));

        //config o fix de saida
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("Ex2_output"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //config os tipos de dados comunicados do mapa para o reduce
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // executar a tarefa
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
