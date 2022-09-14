# Arrow Flight Step Input

A PDI (Pentaho Data Integration) step used to communicate with a Flight Server and retrieve data from it to use as input.

License: Apache 2.0

Last release:

Binary: 

## Installing and Running

#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 11
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

To have the binary file either access the link given previously, or execute this command in the arrow-flight-step-plugin directory and head to the target directory
to access it:

```
mvn clean install
```

To download PDI access the following link and download the 9.3 version: https://sourceforge.net/projects/pentaho/

After installing Pentaho, access its location, and in the plugins folder create
a directory. Inside the directory put a version.xml file (can be pasted from other plugin directories), and the content of the .zip file.

To run Pentaho simply execute the Spoon.bat file by double clicking it, or running an executable on the windows command prompt.

When Pentaho opens, to use the Arrow Flight Input Step, create a transformation and in the ```Design``` tab access the ```Input``` dropdown menu and drag and drop it into the transformation.