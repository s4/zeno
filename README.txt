# I. Installing packages before building

mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=zookeeper -Dversion=3.1.1 -Dpackaging=jar -Dfile=$PATH_TO_ZOOKEEPER_JAR


# II. Build

mvn clean package assembly:assembly


# III. Set up directories in zookeeper.

# 1. Start a ZooKeeper server
# 2. Use the zkCli.sh script provided with Zookeeper

zkCli.sh < /src/main/resources/s4-cluster.cmds


# IV. Start a Zeno Console
# In a separate window (you may omit rlwrap, if it is not installed on the system):
rlwrap java -cp target/zeno-0.1.0.0-jar-with-dependencies.jar:target/zeno-0.1.0.0.jar io.s4.zeno.console.Main localhost /s4cluster

# V. Start some Sites

# In separate windows:
SITE1: java -cp target/zeno-0.1.0.0-jar-with-dependencies.jar:target/zeno-0.1.0.0.jar io.s4.zeno.SiteTest SITE1 localhost /s4cluster "{port.event:12344,port.receive.protocol:21344,port.receive.data:13244}"
SITE2: java -cp target/zeno-0.1.0.0-jar-with-dependencies.jar:target/zeno-0.1.0.0.jar io.s4.zeno.SiteTest SITE2 localhost /s4cluster "{port.event:12345,port.receive.protocol:21345,port.receive.data:13245}"


# VI. Generate Load 

# In the console window (IV)

loadgen src/main/resources/high-100.txt


# VII. Notice that the 2 sites are now overloaded and try to shed load.

# Start a third site to take over the load.
SITE3: java -cp target/zeno-0.1.0.0-jar-with-dependencies.jar:target/zeno-0.1.0.0.jar io.s4.zeno.SiteTest SITE3 localhost /s4cluster "{port.event:12346,port.receive.protocol:21346,port.receive.data:13246}"


# Now observe as parts are transferred.
