const express = require('express');
const pg = require('pg');

const cors = require('cors');

const app = express();

app.use(cors());

const RateLimiter = require('./rate-limiter');

// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html
const pool = new pg.Pool();

// Assumptions/Notes
// * Global rate limit - no specific attributes were identified to limit by, so we will assume that it's a global rate limit (options might include by IP, path, etc)
// * Routes can be visited up to the rate limit (exclusive), then all subsequent requests past the limit will be denied
// * Once a server is started, there is no on-the-fly configuration changes until restarted with a new configuration
// * Cluster mode count reset must be done manually - otherwise, counts will persist through shutdowns/failures
// * Local mode count will be reset upon every restart
// * Rate limiting is on a running basis (i.e. since app/data store initialisation). Other options might store historical counts and a real-time interval reset
// * Route limits might also be stored in Redis if in Cluster mode
// * Configuration and/or schema validation might be added for more robustness

let config = {
  'routeLimits': {
    '/': 10,
    '/events/hourly': 1000,
    '/events/daily': 1000,
    '/stats/hourly': 1000,
    '/stats/daily': 1000,
    '/poi': 1000,
    '/poi/events/hourly': 1000,
    '/poi/events/daily': 1000,
    '/poi/stats/hourly': 1000,
    '/poi/stats/daily': 1000
  },
  'clusterMode': false,
  'connectionString': 'redis://127.0.0.1:6379/0'
};

const rateLimiter = new RateLimiter(config);

const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || []);
  }).catch(next)
}

app.get('/', 
  rateLimiter.limitRate, 
  (req, res, next) => {
    res.send('Welcome to EQ Works 😎')
  }
)

app.get('/events/hourly', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        date,
        hour,
        events
      FROM 
        public.hourly_events
      ORDER BY
        date, hour
      LIMIT 168;
    `
    return next();
  }, 
  queryHandler
)

app.get('/events/daily', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        date, 
        SUM(events) AS events
      FROM 
        public.hourly_events
      GROUP BY 
        date
      ORDER BY
        date
      LIMIT 7;
    `
    return next();
  }, 
  queryHandler
)

app.get('/stats/hourly', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        date,
        hour,
        impressions,
        clicks,
        revenue
      FROM 
        public.hourly_stats
      ORDER BY
        date, hour
      LIMIT 168;
    `
    return next();
  }, queryHandler
)

app.get('/stats/daily', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT date,
          SUM(impressions) AS impressions,
          SUM(clicks) AS clicks,
          SUM(revenue) AS revenue
      FROM public.hourly_stats
      GROUP BY date
      ORDER BY date
      LIMIT 7;
    `
    return next();
  }, 
  queryHandler
)

app.get('/poi',
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT *
      FROM 
        public.poi;
    `
    return next();
  }, 
  queryHandler
)

app.get('/poi/events/hourly', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        poi.poi_id AS poi_id,
        poi.name AS name, 
        poi.lat AS lat,
        poi.lon AS lon,
        hourly.events AS events,
        hourly.date AS date,
        hourly.hour AS hour
      FROM 
        public.poi poi
      JOIN 
        public.hourly_events hourly
      ON 
        poi.poi_id = hourly.poi_id
      GROUP BY 
        poi.poi_id, date, hour
      ORDER BY 
        date, hour
      LIMIT 100;
    `
    return next();
  }, 
  queryHandler
)

app.get('/poi/events/daily', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        poi.poi_id AS poi_id,
        poi.name AS name,
        poi.lat AS lat,
        poi.lon AS lon,
        SUM(hourly.events) AS events,
        hourly.date AS date
      FROM 
        public.poi poi
      JOIN 
        public.hourly_events hourly
      ON 
        poi.poi_id = hourly.poi_id
      GROUP BY 
        poi.poi_id, date
      ORDER BY 
        date
      LIMIT 100;
    `
    return next()
  }, 
  queryHandler
)

app.get('/poi/stats/hourly', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        poi.poi_id AS poi_id, 
        poi.name AS name,
        poi.lat AS lat,
        poi.lon AS lon,
        hourly.impressions AS impressions,
        hourly.clicks AS clicks,
        hourly.revenue AS revenue,
        hourly.date AS date,
        hourly.hour AS hour
      FROM 
        public.poi poi
      JOIN 
        public.hourly_stats hourly
      ON 
        poi.poi_id = hourly.poi_id
      GROUP BY
        poi.poi_id, date, hour
      ORDER BY
        date, hour
      LIMIT 100;
    `
    return next()
  }, 
  queryHandler
)

app.get('/poi/stats/daily', 
  rateLimiter.limitRate,
  (req, res, next) => {
    req.sqlQuery = `
      SELECT 
        poi.poi_id AS poi_id,
        poi.name AS name,
        poi.lat AS lat,
        poi.lon AS lon,
        SUM(hourly.impressions) AS impressions,
        SUM(hourly.clicks) AS clicks,
        SUM(hourly.revenue) AS revenue,
        hourly.date AS date
      FROM 
        public.poi poi
      JOIN 
        public.hourly_stats hourly
      ON 
        poi.poi_id = hourly.poi_id
      GROUP BY
        poi.poi_id, date
      ORDER BY 
        date
      LIMIT 100;
    `
    return next();
  }, 
  queryHandler
)

app.listen(process.env.PORT || 5555, (err) => {
  if (err) {
    console.error(err);
    process.exit(1);
  } else {
    console.log(`Running on ${process.env.PORT || 5555}`);
  }
})

// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`);
  process.exit(1);
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
  process.exit(1);
})
