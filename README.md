AkImporter
==========

Import Tool for the [VuFind/AkSearch](https://github.com/AKBibliothekWien/aksearch) Discovery Tool.

Made and maintained by [Arbeiterkammer Wien](https://github.com/AKBibliothekWien)
This fork only makes some modifications to the pom.xml and the description to facilitate a build from source.

``` bash
# compile to single executable JAR
mvn clean compile assembly:single

# execute initial import
java -jar AkImporter-{yourVersion}.jar -p -v -o

# import new data only
java -jar AkImporter-{yourVersion}.jar -R -v -o


```

Documentation
-------------
Documentation is in german language only!

See: https://emedien.arbeiterkammer.at/wiki/aksearch/dataimport/akimporter
