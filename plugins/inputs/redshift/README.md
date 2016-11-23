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

## Tags:
- The (optional) `cluster_name` configuration parameter provided will be tagged to metrics.


## Example Output:

``` 
./telegraf -config telegraf.conf -test 
* Plugin: redshift, Collection 1
> redshift,cluster=lucid-research,host=phubuntu evicted\ queries=0i,executing\ queries=1i,queued\ queries=2i,serviced\ queries=95814i 1479930340000000000
> redshift,cluster=lucid-research,host=phubuntu columns\ not\ compressed=6812i 1479930340000000000
> redshift,cluster=lucid-research,host=phubuntu analyze\ operations=58i 1479930340000000000
> redshift,cluster=lucid-research,host=phubuntu copy\ -\ load\ lines\ scanned=2383676i 1479930340000000000
> redshift,cluster=lucid-research,host=phubuntu avg\ query\ time\ ms=1311i 1479930341000000000
> redshift,cluster=lucid-research,host=phubuntu currently\ running\ queries=1i 1479930341000000000
> redshift,cluster=lucid-research,host=phubuntu copy\ -\ load\ errors=0i 1479930341000000000
> redshift,cluster=lucid-research,host=phubuntu total\ alerts=150i 1479930342000000000
> redshift,cluster=lucid-research,host=phubuntu total\ packets=14200344i 1479930342000000000
> redshift,cluster=lucid-research,host=phubuntu avg\ analyze\ duration\ sec=0i 1479930342000000000
> redshift,cluster=lucid-research,host=phubuntu avg\ commit\ queue\ size=28i 1479930343000000000
> redshift,cluster=lucid-research,host=phubuntu total\ wlm\ queue\ time\ seconds=822.33806400 1479930343000000000
> redshift,cluster=lucid-research,host=phubuntu database\ connections=39i 1479930345000000000
> redshift,cluster=lucid-research,host=phubuntu queries\ traffic=192i 1479930346000000000
> redshift,cluster=lucid-research,host=phubuntu copy\ -\ unload\ rows=0i 1479930346000000000
> redshift,cluster=lucid-research,host=phubuntu query\ scans\ no\ sort=755i 1479930355000000000
> redshift,cluster=lucid-research,host=phubuntu total\ disk\ based\ queries=7i 1479930366000000000
> redshift,cluster=lucid-research,host=phubuntu max\ skew\ sort\ ratio=100.00,max\ unsorted\ percent=100.00,max\ varchar\ size=65535i,number\ of\ tables\ skew\ sort=34i,number\ of\ tables\ skewed=10i,number\ of\ tables\ stats\ off=480i,tables\ not\ compressed=306i,total\ skew\ sort\ ratio=184.47,total\ table\ rows
```