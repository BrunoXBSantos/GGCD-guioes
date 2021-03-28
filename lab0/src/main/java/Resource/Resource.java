package Resource;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;
import java.util.*;

import static Util.Util.*;

public class Resource {

    private String ex2;
    private String ex3;

    public Resource(String ex2, String ex3){
        this.ex2 = ex2;
        this.ex3 = ex3;
    }

    // calcula o top 10 de generos mais populares  EXERCICIO 2
    public Map<String, Integer> top10PopularGenre() throws IOException, CompressorException {
        CompressorStreamFactory csf = new CompressorStreamFactory();  // read compressed files
        BufferedReader br = new BufferedReader(new InputStreamReader(
                csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.ex2)))));

        String line = br.readLine(); // Skip header
        TreeMap<String, Integer> map = new TreeMap<>();
        while ((line = br.readLine()) != null) {
            String[] lineItems = line.split("\t");   // lista de colunas numa linha
            String genre = lineItems[lineItems.length-1];  // apenas o genero
            String[] g = separate(genre);                  // separo se existir mais que um genero
            for(String a: g){                              // vou adicionando ao map
                if(!map.containsKey(a)){
                    map.put(a,1);
                }
                else {
                    map.put(a, (map.get(a)) + 1);
                }
            }
        }
        // Calling the method valueSort
        Map<String, Integer> sortedMap = valueSort(map);
        // Get a set of the entries on the sorted map
        Set set = sortedMap.entrySet();
        // Get an iterator
        int count = 0;
        Map treeMap = new TreeMap<String,Integer>();
        Iterator i = set.iterator();
        // Display elements
        while (i.hasNext() && count < 5) {
            Map.Entry mp = (Map.Entry)i.next();
            treeMap.put(mp.getKey(), mp.getValue());
            count++;
        }
        return treeMap;
    }

    // Calcule a lista de identificadores de títulos para cada pessoa, ordenados por identificador de pessoa,   ECERCICIO 3
    public TreeMap<String, ArrayList<String>> computeTitle2Person() throws IOException, CompressorException {
        CompressorStreamFactory csf = new CompressorStreamFactory();  // read compressed files
        BufferedReader br = new BufferedReader(new InputStreamReader(
                csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(this.ex3)))));

        TreeMap<String,ArrayList<String>> map = new TreeMap<String,ArrayList<String>>();
        String line = br.readLine(); // Skip header
        while ((line = br.readLine()) != null) {
            String[] lineItems = line.split("\t");   // lista de colunas numa linha
            ArrayList<String> temp = new ArrayList<String>();
            if(!map.containsKey(lineItems[2])){
                temp.add(lineItems[0]);
            }
            else{
                (temp = map.get(lineItems[2])).add(lineItems[0]);
                map.remove(lineItems[2]);
            }
            map.put(lineItems[2],temp);
        }

        return map;

        /*
        int j = 0;
        for(ArrayList<String> a: map.values()){
            for(String b: a){
                j++;
            }
        }
        print("esperado: " + count  + " real: " + j);
        * */

    }



}