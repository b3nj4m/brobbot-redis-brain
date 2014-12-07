{Brain, User} = require '../..'
Url = require "url"
Redis = require "redis"
Q = require "q"
_ = require "lodash"
msgpack = require "msgpack"

class RedisBrain extends Brain
  constructor: (@robot, @useMsgpack = true) ->
    super(@robot)

    redisUrl = if process.env.REDISTOGO_URL?
                 redisUrlEnv = "REDISTOGO_URL"
                 process.env.REDISTOGO_URL
               else if process.env.REDISCLOUD_URL?
                 redisUrlEnv = "REDISCLOUD_URL"
                 process.env.REDISCLOUD_URL
               else if process.env.BOXEN_REDIS_URL?
                 redisUrlEnv = "BOXEN_REDIS_URL"
                 process.env.BOXEN_REDIS_URL
               else if process.env.REDIS_URL?
                 redisUrlEnv = "REDIS_URL"
                 process.env.REDIS_URL
               else
                 'redis://localhost:6379'

    if redisUrlEnv?
      @robot.logger.info "Discovered redis from #{redisUrlEnv} environment variable"
    else
      @robot.logger.info "Using default redis on localhost:6379"

    @info   = Url.parse  redisUrl, true
    @client = Redis.createClient(@info.port, @info.hostname, return_buffers: true)
    @prefix = @info.path?.replace('/', '') or 'brobbot'
    @prefixRegex = new RegExp "^#{@prefix}"

    connectedDefer = Q.defer()
    @connected = connectedDefer.promise

    @client.on "connect", connectedDefer.resolve.bind(connectedDefer)

    @connected.then ->
      @robot.logger.info "Successfully connected to Redis"
    @connected.fail (err) ->
      @robot.logger.error "Failed to connect to Redis: " + err

    if @info.auth
      @authed = Q.ninvoke @client, "auth", @info.auth.split(":")[1]

      @authed.then ->
        @robot.logger.info "Successfully authenticated to Redis"
      @authed.fail ->
        @robot.logger.error "Failed to authenticate to Redis"
    else
      @authed = Q()

    @ready = Q.all [@connected, @authed]

  # Take a dump
  #
  # Returns promise for object
  dump: ->
    #TODO return contents of dump.rdb?

  llen: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'llen', @key key)

  lset: (key, index, value) ->
    @ready.then =>
      Q.ninvoke(@client, 'lset', @key(key), index, @serialize value)

  linsert: (key, placement, pivot, value) ->
    @ready.then =>
      Q.ninvoke(@client, 'linsert', @key(key), placement, pivot, @serialize value)

  lpush: (key, value) ->
    @ready.then =>
      Q.ninvoke(@client, 'lpush', @key(key), @serialize value)

  rpush: (key, value) ->
    @ready.then =>
      Q.ninvoke(@client, 'rpush', @key(key), @serialize value)

  lpop: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'lpop', @key(key)).then @deserialize.bind(@)

  rpop: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'rpop', @key(key)).then @deserialize.bind(@)

  lindex: (key, index) ->
    @ready.then =>
      Q.ninvoke(@client, 'lindex', @key(key), index).then @deserialize.bind(@)

  lrange: (key, start, end) ->
    @ready.then =>
      Q.ninvoke(@client, 'lrange', @key(key), start, end).then (values) =>
        _.map values, @deserialize.bind(@)

  sadd: (key, value) ->
    @ready.then =>
      Q.ninvoke @client, 'sadd', @key(key), value

  sismember: (key, value) ->
    @ready.then =>
      Q.ninvoke @client, 'sismember', @key(key), value

  srem: (key, value) ->
    @ready.then =>
      Q.ninvoke @client, 'srem', @key(key), value

  scard: (key) ->
    @ready.then =>
      Q.ninvoke @client, 'scard', @key(key)

  spop: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'spop', @key(key)).then @deserialize.bind(@)

  srandmember: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'srandmember', @key(key)).then @deserialize.bind(@)

  smembers: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'smembers', @key(key)).then (values) =>
        _.map values, @deserialize.bind(@)

  keys: (searchKey = '') ->
    if searchKey
      prefix = @key searchKey
    else
      prefix = @prefix

    @ready.then =>
      Q.ninvoke(@client, "keys", "#{prefix}:*").then (keys) =>
        _.map keys, (key) => @unkey key

  type: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'type', @key key)

  types: (keys) ->
    @ready.then =>
      Q.all(_.map(keys, (key) => @type key))

  unkey: (key) ->
    key.replace @prefixRegex, ''

  key: (key) ->
    "#{@prefix}:#{key}"

  exists: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'exists', @key(key))

  get: (key) ->
    @ready.then =>
      Q.ninvoke(@client, "get", @key(key)).then(@deserialize.bind(@))

  set: (key, value) ->
    @ready.then =>
      Q.ninvoke(@client, "set", @key(key), @serialize(value))

  remove: (key) ->
    @ready.then =>
      Q.ninvoke(@client, 'del', @key(key))

  # Public: increment the value by num atomically
  #
  # Returns promise
  incrby: (key, num) ->
    @ready.then =>
      Q.ninvoke(@client, 'incrby', @key(key), num)

  # Public: Get all the keys for the given hash table name
  #
  # Returns promise for array.
  hkeys: (table) ->
    @ready.then =>
      Q.ninvoke(@client, 'hkeys', @key(table))

  # Public: Get all the values for the given hash table name
  #
  # Returns promise for array.
  hvals: (table) ->
    @ready.then =>
      Q.ninvoke(@client, 'hvals', @key(table)).then (list) =>
        _.map list, @deserialize.bind(@)

  # Public: Set a value in the specified hash table
  #
  # Returns promise for the value.
  hset: (table, key, value) ->
    @ready.then =>
      Q.ninvoke(@client, 'hset', @key(table), key, @serialize value)

  # Public: Get a value from the specified hash table.
  #
  # Returns: promise for the value.
  hget: (table, key) ->
    @ready.then =>
      Q.ninvoke(@client, 'hget', @key(table), key).then @deserialize.bind(@)

  # Public: Delete a field from the specified hash table.
  #
  # Returns promise
  hdel: (table, key) ->
    @ready.then =>
      Q.ninvoke(@client, 'hdel', @key(table), key)

  # Public: Get the whole hash table as an object.
  #
  # Returns: object.
  hgetall: (table) ->
    @ready.then =>
      Q.ninvoke(@client, 'hgetall', @key(table)).then (obj) =>
        _.mapValues obj, @deserialize.bind(@)

  # Public: increment the hash value by num atomically
  #
  # Returns promise
  hincrby: (table, key, num) ->
    @ready.then =>
      Q.ninvoke(@client, 'hincrby', @key(table), key, num)

  close: ->
    @client.quit()

  # Public: Perform any necessary pre-set serialization on a value
  #
  # Returns serialized value
  serialize: (value) ->
    if @useMsgpack
      return msgpack.pack(value)

    JSON.stringify(value)

  # Public: Perform any necessary post-get deserialization on a value
  #
  # Returns deserialized value
  deserialize: (value) ->
    if @useMsgpack
      return msgpack.unpack(value)

    JSON.parse(value)

  # Public: Perform any necessary pre-set serialization on a user
  #
  # Returns serialized user
  serializeUser: (user) ->
    @serialize user

  # Public: Perform any necessary post-get deserializtion on a user
  #
  # Returns a User
  deserializeUser: (obj) ->
    obj = @deserialize obj

    if obj and obj.id
      return new User obj.id, obj

    null

  # Public: Get an Array of User objects stored in the brain.
  #
  # Returns promise for an Array of User objects.
  users: ->
    @ready.then =>
      Q.ninvoke(@client, 'hgetall', @key('users')).then (users) =>
        _.mapValues users, @deserializeUser.bind(@)

  # Public: Add a user to the data-store
  #
  # Returns promise for user
  addUser: (user) ->
    @ready.then =>
      Q.ninvoke(@client, 'hset', @key('users'), user.id, @serializeUser(user)).then -> user

  # Public: Get or create a User object given a unique identifier.
  #
  # Returns promise for a User instance of the specified user.
  userForId: (id, options) ->
    @ready.then =>
      Q.ninvoke(@client, 'hget', @key('users'), id).then (user) =>
        if user
          user = @deserializeUser user

        if !user or (options and options.room and (user.room isnt options.room))
          return @addUser(new User(id, options))

        return user

  # Public: Get a User object given a name.
  #
  # Returns promise for a User instance for the user with the specified name.
  userForName: (name) ->
    name = name.toLowerCase()

    @users.then (users) ->
      _.find users, (user) ->
        user.name and user.name.toString().toLowerCase() is name

  # Public: Get all users whose names match fuzzyName. Currently, match
  # means 'starts with', but this could be extended to match initials,
  # nicknames, etc.
  #
  # Returns promise an Array of User instances matching the fuzzy name.
  usersForRawFuzzyName: (fuzzyName) ->
    fuzzyName = fuzzyName.toLowerCase()

    @users.then (users) ->
      _.find users, (user) ->
        user.name and user.name.toString().toLowerCase().indexOf(fuzzyName) is 0

  # Public: If fuzzyName is an exact match for a user, returns an array with
  # just that user. Otherwise, returns an array of all users for which
  # fuzzyName is a raw fuzzy match (see usersForRawFuzzyName).
  #
  # Returns promise an Array of User instances matching the fuzzy name.
  usersForFuzzyName: (fuzzyName) ->
    fuzzyName = fuzzyName.toLowerCase()

    @usersForRawFuzzyName(fuzzyName).then (matchedUsers) ->
      exactMatch = _.find matchedUsers, (user) ->
        user.name.toLowerCase() is fuzzyName

      exactMatch or matchedUsers

module.exports = RedisBrain
