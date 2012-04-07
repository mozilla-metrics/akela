REGISTER 'akela-0.3-SNAPSHOT.jar'
REGISTER 'maxmind-geoip-1.2.5.jar'

SET pig.logfile example-geoip.log;

/*
Invoke using these options to run in fully-distributed mode utilizing distributed cache for your GeoIP file:
-Dmapred.cache.archives=hdfs://host:port/path/GeoIP.dat#GeoIP.dat 
-Dmapred.create.symlink=yes
*/
DEFINE GeoIpLookup com.mozilla.pig.eval.geoip.GeoIpLookup('GeoIPCity.dat');
  
data = LOAD '$input' USING PigStorage() AS (ip:chararray);
geo_data = FOREACH data GENERATE ip,
                                 GeoIpLookup(ip) AS location:tuple(country:chararray, country_code:chararray, 
                                                                   region:chararray, city:chararray, 
                                                                   postal_code:chararray, metro_code:int);
STORE geo_data INTO '$output';