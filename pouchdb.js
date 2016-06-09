require('defaultable')(module,
  { prefix: null
  }, function(module, exports, DEFAULT) {


module.exports = get_db
module.exports.uuid = random_uuid

var txn = require('txn')
var async = require('async')
var debug = require('debug')('fun-pouchdb:api')
var PouchDB = require('pouchdb')
var leveldown = require('leveldown')

var prep_ddocs = require('./prep_ddocs.js')
var bulk_docs_validate = require('./bulk_docs_validate.js')


// Fun PouchDB includes Txn, because Txn is nice.
PouchDB.plugin(txn.PouchDB)


// Repeated calls to get_db('foo') will return the same object.
var DB_CACHE = {}


function random_uuid() {
  return PouchDB.utils.uuid()
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
      result[db._fun.name] = db
    callback(null, result)
  }
  
  if (typeof name != 'string')
    throw new Error(`Must provide a name: ${name}`)

  options = options || {}
  var opts = { db      : options.db       || leveldown
             , prefix  : options.prefix   || process.env.POUCHDB_PREFIX || DEFAULT.prefix
             , ddocs   : options.ddocs    || []
             , validate: options.validate || complaining_validator
             }

  if (opts.db === leveldown && !opts.prefix)
    throw new Error(`No directory prefix specified; supply a .prefix option, or set $POUCHDB_PREFIX`)

  if (options.ddoc)
    opts.ddocs.push(options.ddoc)

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
  return new PouchDB(name, opts, function(er, db) {
    if (er)
      return callback(er)

    db._fun = {}
    db._fun.name = name
    db._fun.validate = opts.validate // XXX This could fall out of sync with db.validate.

    // Also stick it in .validate for convenience.
    db.validate = opts.validate

    db.bulkDocs = bulk_docs_validate

    prep_ddocs(db, opts.ddocs, function(er) {
      // Promises are swallowing errors thrown by the callback. For now, just bust out of the promise until I fix this.
      setImmediate(function() {
        debug('About to call callback er=%j', !!er)
        callback(er, db)
      })
    })
  })
}

function complaining_validator(doc) {
  console.log('WARN: No validation function in %s for doc: %j!', this._fun.name, doc)
}

}) // defaultable
