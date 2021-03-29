<div align="center">

# LAB2

</div>

Aqui encontra-se o terceiro guião da UC de gestão de grandes conjundos de dados (GGCD).

### :inbox_tray: Prerequisites

The following software is required to be installed on your system:

- [Java SDK 14](https://openjdk.java.net/)
- [Maven](https://maven.apache.org/maven-features.html)
- [Docker](https://www.docker.com/)
- 

### :hammer: Exercício 1

  ```
  git clone https://github.com/big-data-europe/docker-hadoop.git
  ```
  ```
  docker-compose pull 
  ```
  ```
  docker-compose up
  ```
  
### :hammer: Exercício 2

 Fora de um container e Loading files local
  ```
  docker run --env-file hadoop.env -v /home/bruno-santos/Desktop/GGCD/git/GGCD-guioes/Gz/:/data --network docker-hadoop_default -it bde2020/hadoop-base hdfs dfs -put /data/title.basics.tsv.gz  /
  ```
  ```
  docker-compose up
  
  
curl --output - http://www.imdb.com/interfaces/title.basics.tsv.gz | hdfs dfs -put - /title.basics.tsv.gz
Para correr a APP num container docker

  - criar o ficheiro Dockerfile

  - Criar a imagem lab0
  ```
  docker build -t lab0 . 
  ```

  - Correr o container
  ```
  docker run -it -v /home/bruno-santos/Desktop/GGCD/guioes/lab0/mini/:/data code1 /data/title.basics.tsv.bz2 /data/title.principals.tsv.bz2
 
  ```

### :hammer_and_wrench: Tools

The recommended Integrated Development Environment (IDE) is IntelliJ IDEA.
