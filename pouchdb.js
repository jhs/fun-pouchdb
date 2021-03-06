require('defaultable')(module,
  { prefix: null
  , txn   : {timestamps: true}
  }, function(module, exports, DEFAULT) {


module.exports = get_db
module.exports.uuid = random_uuid

var txn = require('txn').defaults(DEFAULT.txn)
var async = require('async')
var debug = require('debug')('fun-pouchdb:api')
var PouchDB = require('pouchdb-node')
var PouchDBUtils = require('pouchdb-utils')
var leveldown = require('leveldown')

var Cloudant = require('./cloudant.js')
var prep_ddocs = require('./prep_ddocs.js')
var bulk_docs_validate = require('./bulk_docs_validate.js')


PouchDB.plugin(Cloudant)

// Fun PouchDB includes Txn, because Txn is nice.
PouchDB.plugin(txn.PouchDB)


// Repeated calls to get_db('foo') will return the same object.
var DB_CACHE = {}


function random_uuid() {
  return PouchDBUtils.uuid()
}

function get_db(name, options, callback) {
  if (!callback && typeof options == 'function') {
    callback = options
    options = {}
  }

  if (Array.isArray(name)) {
    debug('Get an array of DBs: %j', name)
    return async.map(name, get_one, return_obj)
  }
  
  if (name && typeof name == 'object') {
    debug('Get an object of DBs: %j', name)
    var dbs = Object.keys(name).map(function(db_name) {
      return {name:db_name, options:name[db_name]}
    })
    return async.map(dbs, get_one, return_obj)
  }

  function get_one(opts, to_async) {
    debug('get_one %j', opts)
    if (typeof opts == 'string')
      opts = {name:opts}
    get_db(opts.name, opts.options || options, to_async)
  }

  function return_obj(er, dbs) {
    if (er)
      return callback(er)

    debug('Build an object of %s DBs', dbs.length)
    var result = {}
    for (var db of dbs)
      result[db.fun.name] = db
    callback(null, result)
  }
  
  if (typeof name != 'string')
    throw new Error(`Must provide a name: ${name}`)

  options = options || {}
  var opts = { db      : options.db       || leveldown
             , prefix  : options.prefix   || process.env.POUCHDB_PREFIX || DEFAULT.prefix
             , ddocs   : options.ddocs    || []
             , validate: options.validate || complaining_validator
             , cloudant: options.cloudant || null
             }

  if (opts.db === leveldown && !opts.prefix)
    throw new Error(`No directory prefix specified; supply a .prefix option, or set $POUCHDB_PREFIX`)

  if (options.ddoc)
    opts.ddocs.push(options.ddoc)

  opts.ddocs.push(util_ddoc())

  if (! opts.prefix.endsWith('/'))
    opts.prefix += '/'

  var cache_key = opts.prefix + name
  if (DB_CACHE[cache_key]) {
    debug('Return cached DB: %s', cache_key)
    return setImmediate(function() {
      callback(null, DB_CACHE[cache_key])
    })
  }

  debug('Get DB: %j %j', name, opts)
  // Promises are swallowing errors thrown by the callback. For now, just bust out of the promise until I fix this.
  var db = new PouchDB(name, opts)
  db.info(function(er, info) { setImmediate(function() {
    if (er)
      return callback(er)
    debug('Info for %s: %j', name, info)

    db.fun = {}
    db.fun.name = name
    db.fun.uuid = random_uuid
    db.fun.validate = opts.validate // XXX This could fall out of sync with db.validate.
    db.fun.cloudant = opts.cloudant
    db.fun.warm_view = function(path) { return warm_view(db, path) }

    // Also stick it in .validate for convenience.
    db.validate = opts.validate

    db.bulkDocs = bulk_docs_validate

    prep_ddocs(db, opts.ddocs, function(er) {
      debug('Cache db: %s', cache_key)
      DB_CACHE[cache_key] = db

      if(opts.cloudant && opts.cloudant.account && opts.cloudant.password)
        db.cloudant(opts.cloudant)

      callback(er, db)
    })
  }) }) // setImmediate
}

function warm_view(db, view_path) {
  var warmer_begin = new Date
  return db.query(view_path, {reduce:false, limit:1})
    .then(result => {
      var warmer_end = new Date
      var warm_duration_ms = warmer_end - warmer_begin
      debug('Warmed "%s" in %s ms: %s rows', view_path, warm_duration_ms, result.total_rows)
      return {total_rows:result.total_rows, duration:warm_duration_ms}
    })
    .catch(er => {
      debug('ERROR warming view querying %j', view_path, er)
    })
}

function complaining_validator(doc) {
  console.log('WARN: No validation function in %s for doc: %j!', this.fun.name, doc)
}

function util_ddoc() {
  return {
    _id: '_design/util',
    on_cloudant: true,
    options: {include_design:true},
    views: {
      'conflict': {
        reduce: '_count',
        map: function(doc) {
          if (doc._conflicts) {
            // Emit this winning revision.
            emit(doc._id, doc._rev)

            // Emit all conflicting revisions.
            for (var i = 0; i < doc._conflicts.length; i++)
              emit(doc._id, doc._conflicts[i])
          }
        }
      }
    }
  }
}

}) // defaultable
