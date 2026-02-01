# Installing barch.jar using maven
See Installation instructions in README.md for installing the Java components of barch

to build this example
```
mvn clean package
```
to run (given the location of the build is `../../../cmake-build-relwithdebinfo/`)
```
java -Djava.library.path=../../../cmake-build-relwithdebinfo/ -jar target/barchj-test-1.0-SNAPSHOT-jar-with-dependencies.jar
```