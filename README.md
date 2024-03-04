# Web Crawler

> Sp1d3R | 2024

## Usage

The below snippet is used to set crawler options.

```toml
[crawl_options]
log_file = './crawl.log' # log file
database_location = './databases' # databases
debug = true # enable debug log 
profile = true # start profiler
cache_dir = './data' # page cache
graph_dir = './graphs' # graph folder
index = "./indexes.pkl" # index filename
workers = 8 # number of workers to index
```

The below snippet is used for defining a profile for the crawler.

```toml
[profiles]
[profiles.PROFILE_NAME]
    locations = [ 'https://sp1d3r.vercel.app' ]
    depth = 3
    match = [ Regex Matches ]
    filter = [ Regex filters ]
```

Refer to the `config.toml` file for more example usages.

Crawler:

```bash
$ python crawler -config config.toml
```

## TODO 

- Add Graph frontend

## Finished

- Add Indexing
- Add search engine