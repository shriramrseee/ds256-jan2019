# README #

This is the public git repository for Assignment 1 of DS256 - Scalable Systems for Data Science (Jan 2019) course offered in the Computational and Data Science Department at the Indian Institute of Science. Students are expected to fork this repository for their assignment. Instruction for assignment submission are available at http://cds.iisc.ac.in/courses/ds256/

### Directory Structure ###

* Code
	* src/main/java/in/ds256/Assignment1
		* bipartite.java 
		* color.java
		* conductance.java
		* euler.java
		* ktruss.java
		* pr.java
		* scc.java
		* span.java
		* wcc.java
	* src/main/scala/in/ds256/Assignment1
		* bipartiteX.scala 
		* colorX.scala
		* conductanceX.scala
		* eulerX.scala
		* ktrussX.scala
		* prX.scala
		* sccX.scala
		* spanX.scala
		* wccX.scala
	* pom.xml
* Logs 
	* logs.txt (To be modified by the student. Contents discussed later)
* Project_Report.pdf (To be added by the student)

### Contents of logs.txt file ###
The logs.txt should contain the application ID of the jobs run for each question. The current file is an example that should be modified by the students to contain the appropriate application IDs.
Students are also expected to maintain the output of the command `yarn logs -applicationId <application ID>` for each of the application IDs in the logs.txt file in /home/<-username->/ds256/logs/Assignment1 (this directory is on the linux filesystem and not in hdfs) as a backup in case of any failures.
