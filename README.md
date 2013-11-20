<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Instamo
=======

Introduction
-----------

This is currently a framework for parsing a OSM PBF file into hadoop.
Currently the mapper contains stubs where one would implement functionality for Nodes, Ways, and Relations

To build a fat jar:
```
mvn clean package assembly:single
```

To run
```
hadoop jar OSMPBF-0.0.1-jar-with-dependencies.jar com.bennight.OSMPBFHadoop.OSMPBFJob /hdfs/path/to/planet.osm.pbf
```
(change filenames/paths as requires.  Of course have the jar on your hadoop lib path).


Map Reduce
----------

It's currently built against cdh3u6 - if you are running a different version, make sure you change the pom.
