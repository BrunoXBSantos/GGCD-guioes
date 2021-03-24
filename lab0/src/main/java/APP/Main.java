package APP;

import Resource.Resource;
import org.apache.commons.compress.compressors.CompressorException;

import java.io.*;
import java.util.*;

public class Main {
    // exercicio 2 title.basics.tsv.bz2
    // exercicio 3 title.principals.tsv.bz2


    public static void main(String[] args) throws IOException, CompressorException {
        String ex2 = args[0];
        String ex3 = args[1];
        Resource resource = new Resource(ex2, ex3);

        long start_EX2 = System.currentTimeMillis();

        /* EXERCICIO 2 * */
        System.out.println("#####################  EXERCICIO 2 #####################");
        Map top10PopularGenre = resource.top10PopularGenre();

        Iterator i = top10PopularGenre.entrySet().iterator();
        // Display elements
        while (i.hasNext()) {
            Map.Entry mp = (Map.Entry)i.next();
            System.out.println("\t" + mp.getKey() + ": " + mp.getValue());
        }



        // EXERCICIO 3
        System.out.println("\n");
        System.out.println("#####################  EXERCICIO 3 #####################");

        long start_EX3 = System.currentTimeMillis();

        TreeMap<String, ArrayList<String>> title2Person = resource.computeTitle2Person();

        // Display only 5 elements
        int count = 0;
        Iterator i3 = title2Person.entrySet().iterator();
        while(count<5){
            Map.Entry mp3 = (Map.Entry)i3.next();
            ArrayList<String> value = (ArrayList<String>) mp3.getValue();
            System.out.print("\t" + mp3.getKey() + ":\t" );
            for(String s: value){
                System.out.print(s);
                System.out.print(" ");
            }
            System.out.print("\n");
            count++;
        }

        /*
        * for (Map.Entry<String, ArrayList<String>> stringArrayListEntry : title2Person.entrySet()) {
            System.out.println(stringArrayListEntry.getKey() + "  titulos: " + (title2Person.get(stringArrayListEntry.getKey())).size());
        }*/

        long delay_Ex2 = System.currentTimeMillis() - start_EX2;
        long delay_Ex3 = System.currentTimeMillis() - start_EX3;

        System.out.println("\n");
        System.out.println("Exercicio 2 demorou " + delay_Ex2 + " milissegundos");
        System.out.println("Exercicio 3 demorou " + delay_Ex3 + " milissegundos");

    }
}