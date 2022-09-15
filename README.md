# Arrow Flight Step Input

A PDI (Pentaho Data Integration) step used to communicate with a Flight Server and retrieve data from it to use as input.

License: Apache 2.0

Last release: https://github.com/WhoIsGinja/ArrowFlightStep/releases/tag/v0.1

Build: https://github.com/WhoIsGinja/ArrowFlightStep/releases/download/v0.1/arrow-flight-step-plugin-8.1.0.0-365.zip

## Obtaining Binary

#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 11
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

To have the binary file either access the link given previously, or execute this command in the arrow-flight-step-plugin directory and head to the target directory
to access it:

```
mvn clean install
```

## Installing Software

To download PDI, access the following link and download the 9.3 version: https://sourceforge.net/projects/pentaho/

After installing Pentaho, access its location, and in the plugins folder create
a directory. Inside the directory put a version.xml file (can be pasted from other plugin directories), and the content of the .zip file.

To run Pentaho simply execute the Spoon.bat file by double clicking it, or running an executable on the windows command prompt.

When Pentaho opens, to use the Arrow Flight Input Step, create a transformation and in the ```Design``` tab access the ```Input``` dropdown menu and drag and drop it into the transformation.
  
#### Shell command-line example to install the required software:
```
# download pentaho distro and unzip to some dir
mkdir pentaho
cd pentaho/
wget https://sourceforge.net/projects/pentaho/files/Pentaho-9.3/client-tools/pdi-ce-9.3.0.0-428.zip/download
unzip pdi-ce-client-9.3.0.0-428

# download the plugin package and  unzip its contents
wget https://github.com/WhoIsGinja/ArrowFlightStep/releases/download/v0.1/arrow-flight-step-plugin-8.1.0.0-365.zip
unzip arrow-flight-step-plugin-8.1.0.0-365.zip

# move plugin file (and libs) to  pentaho pdi's data-integration/plugins directory
mv arrow-flight-step-plugin-8.1.0.0-365  pdi-ce-client-9.3.0.0-428/data-integration/plugins/

cd pdi-ce-client-9.3.0.0-428/data-integration/
./spoon.sh
```
