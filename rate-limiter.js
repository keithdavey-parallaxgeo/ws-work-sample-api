const _ = require('lodash');
var Redis = require('ioredis');

class RateLimiter {
  constructor(config) {
    this.clusterMode = config.clusterMode;
    this.connectionString = config.connectionString;
    this.getPathCounts = null;
    this.redis = null;
     
    // Set the appropriate set/get based on cluster mode option
    this.limitRate = null;

    // Set up cluster/non-cluster modes
    if (this.clusterMode) {
      // If max retries is reached, we exit with an error code as connection can not be made
      this.redis = new Redis(this.connectionString);
      _.forEach(_.keys(config.routeLimits), (key) => {
        // Initialise redis with routes if they do not already exist
        this.redis.setnx(`routeCounts:${key}`, 0);
      })
      this.limitRate = this.limitRateCluster;
    } else {
      this.limitRate = this.limitRateNonCluster;

      // Map the set of valid (not disabled) route paths to an array to keep track of count
      this.routeCounts = _.mapValues(config.routeLimits, () => {
        return 0;
      })
    }

    // Set the route limit for each configured route
    this.routeLimits = config.routeLimits;
  }

   // Limit rate code for non-cluster mode
   limitRateNonCluster = (req, res, next) => {
    let routePath = req.path;
    if(!_.has(this.routeCounts, routePath)) {
      res.status(429).json({'statusCode': 400, 'message': `Route path ${routePath} not configured.`});
    } else if (this.routeCounts[routePath] < this.routeLimits[routePath]) {
      this.routeCounts[routePath] += 1;
      next();
    } else {
      res.status(429).json({'statusCode': 429, 'message': `Route path ${routePath} has reached its request limit.`});
    }
  }

  // Limit rate code for cluster mode
  limitRateCluster = async (req, res, next) => {
    let routePath = req.path;
    let isValidPath = await this._pathIsValidForLimit(routePath);
    if(!isValidPath) {
      res.status(429).json({'statusCode': 400, 'message': `Route path ${routePath} not configured.`});
    } else if (await this._getPathCountCluster(routePath) < this.routeLimits[routePath]) {
      await this._incrementPathCountCluster(routePath);
      next();
    } else {
      res.status(429).json({'statusCode': 429, 'message': `Route path ${routePath} has reached its request limit.`});
    }
  }

  // Check that the path in the cluster data store is configured for use
  _pathIsValidForLimit = async (path) => {
    let isValidLimit = await this.redis.exists(`routeCounts:${path}`);
    let isValidBooleanLimit = ((isValidLimit === 1) ? true : false);
    return isValidBooleanLimit;
  }

  // Get cluster data store count for *path*
  _getPathCountCluster = async (path) => {
    let count = await this.redis.get(`routeCounts:${path}`);
    return count;
  }

  // Increment the *path* cluster data store count
  _incrementPathCountCluster = async (path) => {
    await this.redis.incr(`routeCounts:${path}`);
    return;
  }
}

module.exports = RateLimiter