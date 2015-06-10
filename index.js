var brobbot = require('brobbot');
var Brain = brobbot.Brain;
var User = brobbot.User;
var Url = require("url");
var Redis = require("redis");
var Q = require("q");
var _ = require("lodash");
var msgpack = require("msgpack");

function RedisBrain(robot, useMsgpack) {
  Brain.call(this, robot);

  var self = this;
  var redisUrl = null;
  var urlEnv = null;
  var envVars = ['REDISTOGO_URL', 'REDISCLOUD_URL', 'BOXEN_REDIS_URL', 'REDIS_URL'];

  this.robot = robot;
  this.useMsgpack = useMsgpack === undefined ? true : useMsgpack;

  for (var i = 0; i < envVars.length; i++) {
    if (process.env[envVars[i]]) {
      urlEnv = envVars[i];
      redisUrl = process.env[envVars[i]];
      break;
    }
  }

  if (!redisUrl) {
    redisUrl = 'redis://localhost:6379';
  }

  if (urlEnv) {
    this.robot.logger.info("Discovered redis from " + urlEnv + " environment variable");
  }
  else {
    this.robot.logger.info("Using default redis on localhost:6379");
  }

  this.info = Url.parse(redisUrl, true);

  this.client = Redis.createClient(this.info.port, this.info.hostname, {
    return_buffers: true
  });

  this.prefix = ((_ref2 = this.info.path) != null ? _ref2.replace('/', '') : void 0) || 'brobbot';
  this.dataPrefix = process.env.BROBBOT_REDIS_DATA_PREFIX || 'data';
  this.prefixRegex = new RegExp("^" + this.prefix + ":");
  this.dataPrefixRegex = new RegExp("^" + this.dataPrefix + ":");

  var connectedDefer = Q.defer();
  this.connected = connectedDefer.promise;
  this.client.on("connect", connectedDefer.resolve.bind(connectedDefer));

  this.connected.then(function() {
    return self.robot.logger.info("Successfully connected to Redis");
  });

  this.connected.fail(function(err) {
    return self.robot.logger.error("Failed to connect to Redis: " + err);
  });

  if (this.info.auth) {
    this.authed = Q.ninvoke(this.client, "auth", this.info.auth.split(":")[1]);
    this.authed.then(function() {
      return this.robot.logger.info("Successfully authenticated to Redis");
    });
    this.authed.fail(function() {
      return this.robot.logger.error("Failed to authenticate to Redis");
    });
  }
  else {
    this.authed = Q();
  }
  this.ready = Q.all([this.connected, this.authed]);
}

RedisBrain.prototype = Object.create(Brain.prototype);
RedisBrain.prototype.constructor = RedisBrain;

RedisBrain.prototype.reset = function() {
  var self = this;

  return this.keys().then(function(keys) {
    return Q.all(_.map(keys, function(key) {
      return self.remove(key);
    }));
  });
};

RedisBrain.prototype.llen = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'llen', self.key(key)).then(function(val) {
      return parseInt(val.toString());
    });
  });
};

RedisBrain.prototype.lset = function(key, index, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lset', self.key(key), index, self.serialize(value));
  });
};

RedisBrain.prototype.linsert = function(key, placement, pivot, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'linsert', self.key(key), placement, self.serialize(pivot), self.serialize(value)).then(function(val) {
      return parseInt(val.toString());
    });
  });
};

RedisBrain.prototype.lpush = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lpush', self.key(key), self.serialize(value));
  });
};

RedisBrain.prototype.rpush = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'rpush', self.key(key), self.serialize(value));
  });
};

RedisBrain.prototype.lpop = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lpop', self.key(key)).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.rpop = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'rpop', self.key(key)).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.lindex = function(key, index) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lindex', self.key(key), index).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.lgetall = function(key) {
  return this.lrange(key, 0, -1);
};

RedisBrain.prototype.lrange = function(key, start, end) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lrange', self.key(key), start, end).then(function(values) {
      return _.map(values, self.deserialize.bind(self));
    });
  });
};

RedisBrain.prototype.lrem = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'lrem', self.key(key), 0, self.serialize(value)).then(function(count) {
      return parseInt(count.toString());
    });
  });
};

RedisBrain.prototype.sadd = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'sadd', self.key(key), self.serialize(value)).then(function(val) {
      return Q();
    });
  });
};

RedisBrain.prototype.sismember = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'sismember', self.key(key), self.serialize(value)).then(function(val) {
      return parseInt(val.toString()) === 1;
    });
  });
};

RedisBrain.prototype.srem = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'srem', self.key(key), self.serialize(value));
  });
};

RedisBrain.prototype.scard = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'scard', self.key(key)).then(function(size) {
      return parseInt(size.toString());
    });
  });
};

RedisBrain.prototype.spop = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'spop', self.key(key)).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.srandmember = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'srandmember', self.key(key)).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.smembers = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'smembers', self.key(key)).then(function(values) {
      return _.map(values, self.deserialize.bind(self));
    });
  });
};

RedisBrain.prototype.keys = function(searchKey) {
  var self = this;

  if (searchKey == null) {
    searchKey = '';
  }

  searchKey = this.key(searchKey);

  return this.ready.then(function() {
    return Q.ninvoke(self.client, "keys", searchKey + "*").then(function(keys) {
      return _.map(keys, function(key) {
        return self.unkey(key.toString());
      });
    });
  });
};

RedisBrain.prototype.type = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'type', self.key(key)).then(function(result) {
      result = result.toString();

      if (result === 'string') {
        return 'object';
      }

      return result;
    });
  });
};

RedisBrain.prototype.types = function(keys) {
  var self = this;

  return this.ready.then(function() {
    return Q.all(_.map(keys, function(key) {
      return self.type(key);
    }));
  });
};

RedisBrain.prototype.unkey = function(key) {
  return key.replace(this.prefixRegex, '').replace(this.dataPrefixRegex, '');
};

RedisBrain.prototype.key = function(key) {
  return this.prefix + ":" + this.dataPrefix + ":" + key;
};

RedisBrain.prototype.usersKey = function() {
  return this.prefix + ":users";
};

RedisBrain.prototype.exists = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'exists', self.key(key)).then(function(exists) {
      return exists.toString() === '1';
    });
  });
};

RedisBrain.prototype.get = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, "get", self.key(key)).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.set = function(key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, "set", self.key(key), self.serialize(value));
  });
};

RedisBrain.prototype.remove = function(key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'del', self.key(key));
  });
};

RedisBrain.prototype.incrby = function(key, num) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'incrby', self.key(key), num).then(function(val) {
      return parseInt(val.toString());
    });
  });
};

RedisBrain.prototype.hkeys = function(table) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hkeys', self.key(table)).then(function(results) {
      return _.map(results, function(result) {
        return result.toString();
      });
    });
  });
};

RedisBrain.prototype.hvals = function(table) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hvals', self.key(table)).then(function(list) {
      return _.map(list, self.deserialize.bind(self));
    });
  });
};

RedisBrain.prototype.hlen = function(table) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hlen', self.key(table)).then(function(val) {
      return parseInt(val.toString());
    });
  });
};

RedisBrain.prototype.hset = function(table, key, value) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hset', self.key(table), key, self.serialize(value));
  });
};

RedisBrain.prototype.hget = function(table, key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hget', self.key(table), key).then(self.deserialize.bind(self));
  });
};

RedisBrain.prototype.hdel = function(table, key) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hdel', self.key(table), key);
  });
};

RedisBrain.prototype.hgetall = function(table) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hgetall', self.key(table)).then(function(obj) {
      var map = new Map();

      _.each(obj, function(val, key) {
        return map.set(key, self.deserialize(val));
      });

      return map;
    });
  });
};

RedisBrain.prototype.hincrby = function(table, key, num) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hincrby', self.key(table), key, num).then(function(val) {
      return parseInt(val.toString());
    });
  });
};

RedisBrain.prototype.close = function() {
  return this.client.quit();
};

RedisBrain.prototype.serialize = function(value) {
  if (this.useMsgpack) {
    if (_.isObject(value)) {
      return msgpack.pack(value);
    }
    return value.toString();
  }

  return JSON.stringify(value);
};

RedisBrain.prototype.deserialize = function(value) {
  if (value === undefined || value === null) {
    return null;
  }

  if (this.useMsgpack) {
    var result = msgpack.unpack(value);

    if (result === undefined || !_.isObject(result)) {
      result = value.toString();
    }

    return result;
  }
  return JSON.parse(value.toString());
};

RedisBrain.prototype.serializeUser = function(user) {
  return this.serialize(user);
};

RedisBrain.prototype.deserializeUser = function(obj) {
  obj = this.deserialize(obj);

  if (obj && obj.id) {
    return new User(obj.id, obj);
  }

  return null;
};

RedisBrain.prototype.users = function() {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hgetall', self.usersKey()).then(function(users) {
      return _.mapValues(users, self.deserializeUser.bind(self));
    });
  });
};

RedisBrain.prototype.addUser = function(user) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hset', self.usersKey(), user.id, self.serializeUser(user)).then(function() {
      return user;
    });
  });
};

RedisBrain.prototype.userForId = function(id, options) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'hget', self.usersKey(), id).then(function(user) {
      if (user) {
        user = self.deserializeUser(user);
      }

      if (!user || (options && options.room && (user.room !== options.room))) {
        return self.addUser(new User(id, options));
      }

      return user;
    });
  });
};

RedisBrain.prototype.userForName = function(name) {
  name = name && name.toLowerCase() || '';

  return this.users().then(function(users) {
    return _.find(users, function(user) {
      return user && user.name.toLowerCase() === name;
    }) || null;
  });
};

RedisBrain.prototype.usersForRawFuzzyName = function(fuzzyName) {
  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.users().then(function(users) {
    return _.filter(users, function(user) {
      return user && user.name.toLowerCase().substr(0, fuzzyName.length) === fuzzyName;
    });
  });
};

RedisBrain.prototype.usersForFuzzyName = function(fuzzyName) {
  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.usersForRawFuzzyName(fuzzyName).then(function(matchedUsers) {
    var exactMatch = _.find(matchedUsers, function(user) {
      return user && user.name.toLowerCase() === fuzzyName;
    });

    return exactMatch && [exactMatch] || matchedUsers;
  });
};

module.exports = RedisBrain;
