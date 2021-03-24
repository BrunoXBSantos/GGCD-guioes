<div align="center">

# LAB0

[Geeting Started](#rocket-getting-started)
|
[Development](#hammer-development)
|
[Tools](#hammer_and_wrench-tools)
|
[Team](#busts_in_silhouette-team)

</div>

Aqui encontra-se o primeiro guião da UC de gestão de grandes conjundos de dados (GGCD).

### :inbox_tray: Prerequisites

The following software is required to be installed on your system:

- [Java SDK 14](https://openjdk.java.net/)
- [Maven](https://maven.apache.org/maven-features.html)
- [Docker](https://www.docker.com/)

### :hammer: Development

Para realizar um JAR da aplicação: 

  - Ver os JAR existentes
  ```
  ls -la target 
  ```
  
  - Construir o JAR
  ```
  java -jar target/lab0-1.0-SNAPSHOT.jar 
  ```
  
  - executar o JAR
  ```
  java -jar target/lab0-1.0-SNAPSHOT.jar mini/title.basics.tsv mini/title.principals.tsv
  ```
  
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

