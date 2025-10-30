
Reference:
https://kafka.apache.org/41/documentation/streams/tutorial


Prerequisites
-JRE 11+
- Maven 3.6+

Generate Project
Run generate.sh to create Java Project with Maven including Kafka-Streams dependency.

Change 
StreamsConfig.BOOTSTRAP_SERVERS_CONFIG value in src/main/java/org/example/WordCountApp.java to your Kafka server address, e.g.:
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkaServer:9092");

Add this plugin to maven pom.xml to create a fat jar:
``` xml
    <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.3.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                        <configuration>
                            <archive>
                                <manifest>
                                    <mainClass>
                                        tap.WordCount
                                    </mainClass>
                                </manifest>
                            </archive>
                            <descriptorRefs>
                                <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
```

Build Project using Docker
``` bash
./build.sh
```