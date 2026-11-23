The first time I used Athena felt magical. I could join tables with billions of rows, and get useful results back all within about 5 seconds. 

Years later I discovered DuckDB, again magical. Just throw multi-gb parquet (or even CSV!) and it would give results back in no time.

I wanted to understand how these things work, and so I initially set myself this challenge:

Build a file format similar to Parquet, but simpler. And a basic query engine that can run the ClickBench queries in less than 100kb of WASM. 

<aside on clickbench>

Clickbench [https://github.com/ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench) is a great open benchmark with about 43 realistic queries and a dataset with 100 million rows. 

</aside>

I achived this (well slightly over 100kb) and managed to beat DuckDB on a few queries! Write up here. However while this was a fun experiment it's mainly a toy project without many practical use cases.

Recently I've been obsessed with efficienciy and where possible full scale to zero. I like Cloudflare's Durable Object  primitive as it gives me the low latency start-up/scale to 0 of a worker, BUT can store state and is individually addressable. 

I decided to build on my 100kb wasm experiment with the following goals:

1. Can we achieve Athena (trinio) and BigQuery performance with purely serverless memory constrained Duable Objects
2. Can the cost be signficantly less than Athena/BigQuery

The result - yes, it is possible to match Athena with memory constrained Durable Objects and its infinitely* scaleable! (infinite = unlimited budget and cloudflare having unlimited capacity).

 The rest of this post goes into how I achieved this.

**The Curse of Rust Dependencies**

Now as someone who has done a load of JS, I can hardly complain about Rust crates vs NPM packages, but I'm going to anyway. The beauty of Rust is the strict compiler with proper type checking (not the false sense of security that you get from TS). BUT, it is very easy for a relatively simple Rust project to end up taking minutes to compile by the time you add a few dependencies. Something like Arrow is particuarly bad (details on arrow deps and how long to compile). In addition a lot of Rust crates are not really made for WASM or a memory constrained environment. So given we are in the world of agents I feel I have full permission to ingore any shame in NIH and just build (prompt) everything from scratch. 

This meant:

1. Custom zero-allocation Thrift parser
2. Custom streaming Parquet page reader
3. Custom JS-Wasm stream pool interface
4. Custom SQL parser (I maanged to get DataFusion down to a reasonaable size, but got sick of the slow iteration and so opted to use my custom parser for experiments, with the ability to swap in DataFusion later)

Claude created some good interactive walk throughs of these here

But let me walk you through the high level architecture first and then data flow with trade offs.

Architecture

The system is built around a few core principles:

1. Instead of distributing work to threads or networked nodes, a Durable Object is the compute primitive used. We separate Scans, Reducers, Finalizers, Coordinators all into diffrent Durable Objects.
2. Communication happens via RPC streams - Cloudflare provides the ability for us to open up low latency TCP streams between these DOs (this is one of the main benefits of DOs vs standard Workers - I only want a single instance of a "reducer", I don't want 10 scanners speaking to 10 instances of the same reducer)
3. Tight memory accounting - each DO can get up to 128 MB, BUT may end up sharing an isolate with another DO and so in practice 64 MB is the ceiling we aim for.
4. Where memory growth is inevidtable (reducer hash tables), the query planner aims to keep memory needed to < 32 MB per DO, BUT where this is missed (i.e. skewed distributions) we spill to R2 (initially I wanted to spill to the DO's sqlite, but its insanely expensive!).
5. To stay on top of memory budgets communication between DOs is via 64kb frames

A few unexpected gotchas I found while developing:

1. Big difference between hibernated DO startup and new DO startup (obvious in retrospect, but I assumed it would be 10-20ms difference, whereas in reality hibernated DOs can start in about 120ms, whereas fresh DOs where more like 300ms.
2. CPU allocation per DO is not uniform - this could be expected, but I thought it would be random, whereas in reality I had some DO's that consistently had demonstrably slower CPU performance than the rest.

So how does it work

A query is submitted to the co-ordinator which parses, creates a logical plan, reads the parquet footer(s) and creates a physical plan. The physical plan is a graph of Scan, Reduce and Finalise nodes. Durable objects are initlised with the graph and their particular job. Because durable objects are addressable a scan node can create connectiosn to reducer nodes, and reducer nodes can create connections to finaliser node (normally the same node as the coordinator). 



Each Scan node receives R2 ranges to read, and it performs filtering and projection and then sends frames to up to 100 reducers. Each reducer receives streams from up to 100 scanners, processes and builds up its hash table - this is the same mechanism for global sort, join, group by, top K, etc. If the hash table breaches the memory limit it is sorted and streamed to R2. This can happen multiple times, so for a really bad skew we could need up to 10 x 32mb sorted hash table partials on R2. Once all Scan streams have been received the Reducer then reads the 10 paralell sorted hash table partials in paralell, joins and emits to the finaliser. For TopK queries - Reducers only need to emit their local TopK, which reduces the data sent signifcantly. 

There isn't much more than that, the hardest parts are managing memory and reliable streaming. However when it works its beautiful and a lot more visible than a standard multi-core threaded query engine. 

Scaling? To keep reliability I limited input and output streams to maxium 100. This means that if we need 1000 reducers for a really big query we will need to put 100 routers between the reducers and the scanners. The routers take hash % 100 each and then route to their assigned reducers hash % 10. 

So what about cost?

So the numbers, here are clickbench queries.





## Hash Exchange and Spill

While we can write memory efficent code with streaming and strict rules around allocation, some queries or analysis problems just require a lot of data in memory. For example grouping, sorting, joining. There are 2 approaches impleemtned in WCOL, both are standard approaches used in many engines, but how they work with Durable Objects is slightliy different.

The first is a hash exchange. Because we have separate scanner reading the data, we can get those scanners to speak to multiple reducers, and we send to a reducer by hashing the group keys (or sort range, or join keys). So if we are grouping 1 billion events by 1 million users, we need to have a million users in memory as we stream the billion events through the system. The appraoch here is to hash the million users usch that we can divide the million users up among X reducers. e.g. with 100 reducers each reducer only needs to deal with 10,000 users. Essentially we end up with a partitoned hash map if transactions by user, but partitioning allows it to be split across hundreds of durable objects.

Now, if data is even distributed our planner can ensure that each reducer can execute a full query within its limited memory budget. However real data is seldom evenly distributed and therefore we are likely to get some reducers that end up with more data than they can fit in memory. While our planner tries to avoid this happening we need to deal with it when it happens. The appraoch here is to "spill to disk". This is standard practice for query engines, however Durable Objects don't have a disk. I experimented with using the SQlite attached to each Durable Object, this worked but was super expensive - and the aim of this work is to be cheap! So I ended up spilling to R2. 

How does this work. I tried a few approaches, but the simplest one was:

1. Have a fixed size hash 
2. Once it reaches its max size, stop processing incoming streams
3. Sort the hash and stream to R2
4. Start a new hash and start processing incoming streams again

This may repeat multiple times so for a given query we could end up with several partial sorted hashmaps written to R2. Once incoming streams are finished and all the data has been receivied, we then stream all the partial hashmaps back from R2, because they are sorted we can join matching hash keys and output to the finalizer without needing to have the full hashmap in memory. 

This slows the system down and so we try and avoid it where possible, but it makes the system robust (and able to operate on these memory constrained compute instances). 



Interesting problems that were solved

1. Efficient R2 range requests
2.