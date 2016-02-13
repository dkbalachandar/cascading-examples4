This is a java program to read a text file which contains user name and age details and remove the users whose age is more than or equals to 30 using Cascading API

To run this program, follow the below steps,

1. Clone this repository

2. Then build the jar by running mvn package and copy it into hadoop installed directory[/usr/local/hadoop/userLib]. Provide appropriate file permissions

3. Then login as Hadoop user and go to Hadoop directory[/usr/local/hadoop]

4. Copy the user.txt file available in the resources folder into hadoop by running the below command,

      bin/hadoop fs -copyFromLocal user.txt /example/user.txt

5. Run the jar
    bin/hadoop jar userLib/cascading-examples4-1.0-jar-with-dependencices.jar /example/user.txt /example/output
