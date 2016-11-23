# Redshift plugin

This redshift plugin provides metrics for your AWS Redshift clusters.

## Getting started :

You have to create a login on every cluster you want to monitor, with following script:
- **WARNING**:  A major drawback of hitting these system views, is that they require the user to be a SuperUser.   Internally in the AWS Redshift API, if you are not a superuser, trying to retrieve rows from some of these views internally filters on the user's `usesysid`.  This means unless the user is a SuperUser, it would only be able to monitor it's _own_ metrics, i.e. queries coming from the telgraf user itself.
```SQL 
create user telegraf with password 'SomeSekritP@ssword1'
alter user telegraf createuser; -- superuser
```


## Configuration:

``` 
# Read metrics from AWS Redshift
[[inputs.redshift]]
  ## Specify a Redshift cluster to monitor with an address, or connection string.
  ## cluster_name is the optional name of the Redshift cluster
  ## interval_seconds is used for querying windows of metrics
  
  address = "dbname='<db>' port='<p>' user='<user>' password='<pw>' host='<cluster>.<region>.redshift.amazonaws.com'"
  cluster_name = "lucid"
  interval_seconds = 500
```


## Measurement | Fields:

	  
## Tags:
- The (optional) `cluster_name` configuration parameter provided will be tagged to metrics.

	

## Example Output:

``` 
./telegraf -config telegraf.conf -test 
* Plugin: redshift, Collection 1
```