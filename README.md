# A maintenance-tolerant distributed-sleep framework!

Sleep is essential for a person's health and wellbeing, according to the National Sleep Foundation (NSF).
Yet millions of people do not get enough sleep and many suffer from lack of sleep.
For example, surveys conducted by the NSF (1999-2004) reveal that at least 40 million Americans suffer from over 70 different sleep disorders and 60 percent of adults report having sleep problems a few nights a week or more.
Most of those with these problems go undiagnosed and untreated.
In addition, more than 40 percent of adults experience daytime sleepiness severe enough to interfere with their daily activities at least a few days each month - with 20 percent reporting problem sleepiness a few days a week or more.
Furthermore, 69 percent of children experience one or more sleep problems a few nights or more during a week.

Since sleep is so important, we try to maintain a minimum number of sleep tasks active at all times.
However, if our resting places (agents) need cleaning (maintenance), we obviously need to move and sleep elsewhere.

## Installation (OSX)
```
brew install boost
brew install protobuf250
brew install glog
wget -O /usr/local/include/picojson.h https://raw.githubusercontent.com/kazuho/picojson/rel/v1.3.0/picojson.h
```

## Build
```
make
```

## Run
```
example --master <host:port> -n <number of sleep tasks>
```

## Open the Mesos UI

Run:
```
python schedule.py <host:port>
```

Watch tasks get moved around.

## References

* Most of the framework code is based on the [example framework here](https://github.com/apache/mesos/blob/master/src/examples/event_call_framework.cpp).
* Intro paragraph copied from [here](http://www.apa.org/topics/sleep/why.aspx) :).
