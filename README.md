brobbot-redis-brain
===================

A redis-backed brain for [brobbot](https://npmjs.org/package/brobbot).

## Usage

In your [brobbot-instance](https://github.com/b3nj4m/brobbot-instance):

```bash
npm install --save brobbot-redis-brain
./index.sh -b redis
```

## Configuration

### URL

Use one of the following environment variables to set the Redis URL:

- `REDISTOGO_URL`
- `REDISCLOUD_URL`
- `BOXEN_REDIS_URL`
- `REDIS_URL`

```bash
REDISTOGO_URL=redis://user:password@localhost:port ./index.sh -b redis
```

### Data key prefix

Set `BROBBOT_REDIS_DATA_PREFIX` to change the default key prefix (`'data:'`).

```bash
BROBBOT_REDIS_DATA_PREFIX=brobbot-data: ./index.sh -b redis
```
