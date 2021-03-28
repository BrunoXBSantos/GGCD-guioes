package Main;

import MapReduce.Ex4;
import Utilis.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Ex4_Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // config uma tarefa
        Job job = Job.getInstance(new Configuration(), "Ex4");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job.setJarByClass(Ex4.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.basics.tsv"),
                TextInputFormat.class, Ex4.Ex4LeftMapper.class);
        MultipleInputs.addInputPath(job, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.ratings.tsv"),
                TextInputFormat.class, Ex4.Ex4RightMapper.class);
        job.setReducerClass(Ex4.Ex4Reduce.class);

        //config o fix de saida
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("Ex4_output"));

        //config os tipos de dados comunicados do mapa para o reduce
        // maybe not necessary
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // executar a tarefa
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
