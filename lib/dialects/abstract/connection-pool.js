'use strict';

var Pooling = require('generic-pool')
  , _ = require('lodash');

var ConnectionPool = function(config, dialect, sequelize) {
  this.config = config;
  this.dialect = dialect;
  this.sequelize = sequelize;
  this.pools = {};

  // Duplicate the base config into each database
  var defaultConfig = _.extend({}, config);
  delete defaultConfig.databases;

  for (var key in config.databases) {
    config.databases[key] = _.extend({}, defaultConfig, config.databases[key]);
  }
};

ConnectionPool.prototype.acquire = function(priority, queryType, poolName) {
  return this.getPool(poolName).acquire(priority, queryType)
};

ConnectionPool.prototype.release = function(connection) {
  return this.pools[connection.poolName].release(connection);
};

ConnectionPool.prototype.destroy = function(connection) {
  return this.pools[connection.poolName].destroy(connection);
};

ConnectionPool.prototype.destroyAllNow = function() {
  for (var key in this.pools) {
    this.pools[key].destroyAllNow();
  }
};

ConnectionPool.prototype.drain = function(callback) {
  var pending = 0;
  for (var key in this.pools) {
    pending++;
    this.pools[key].drain(function() {
      pending--;
      if (pending === 0) {
        callback();
      }
    });
  }
};

ConnectionPool.prototype.getPool = function(name) {
  var self = this;
  if (name in self.pools) {
    return self.pools[name];
  }

  if (!(name in self.config.databases)) {
    throw new Error('Unknown database ' + name);
  }

  var pool = Pooling.createPool({
    name: 'sequelize-connection-' + name,
    create: () => this._connect(name).catch(err => err),
    destroy: mayBeConnection => {
      if (mayBeConnection instanceof Error) {
        return Promise.resolve();
      }

      return this._disconnect(mayBeConnection)
    },
    validate: self.config.pool.validate
  }, {
    Promise: self.config.pool.Promise,
    testOnBorrow: true,
    returnToHead: true,
    autostart: false,
    max: self.config.pool.max,
    min: self.config.pool.min,
    acquireTimeoutMillis: self.config.pool.acquire,
    idleTimeoutMillis: self.config.pool.idle,
    evictionRunIntervalMillis: self.config.pool.evict
  });

  this.pools[name] = pool;
  return pool;
};

ConnectionPool.prototype._connect = function (name) {
  var config = this.config.databases[name];
  return this.sequelize.runHooks('beforeConnect', config)
    .then(() => this.dialect.connectionManager.connect(config))
    .then(connection => {
      connection.poolName = name;
      this.sequelize.runHooks('afterConnect', connection, config);
      return connection;
    });
}

ConnectionPool.prototype._disconnect  = function (connection) {
  return this.dialect.connectionManager.disconnect(connection);
}
module.exports = ConnectionPool;
