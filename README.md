# Web Crawler

> Sp1d3R | 2024

## Usage

The below snippet is used to set crawler options.

```toml
[crawl_options]
log_file = './crawl.log'
database_location = './crawl.db'
debug = false
```
The below snippet is used for defining a profile for the crawler.

```toml
[profiles]
[profiles.my_website]
    locations = [
        'https://sp1d3r.vercel.app'
    ]

    depth = 3
    method = 'hash'  # TODO: cosine
    same_domain = true  # Todo: regex match
```

Refer to the `config.toml` file for more example usages.