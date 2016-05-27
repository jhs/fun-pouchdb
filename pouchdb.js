module.exports = get_db
module.exports.uuid = random_uuid

var txn = require('txn')
var debug = require('debug')('fun-pouchdb:api')
var PouchDB = require('pouchdb')
var leveldown = require('leveldown')

var prep_ddocs = require('./prep_ddocs.js')
var bulk_docs_validate = require('./bulk_docs_validate.js')

var PREFIX = __dirname + '/_data/'


// Fun PouchDB includes Txn, because Txn is nice.
PouchDB.plugin(txn.PouchDB)


// The user might instantiate a database twice, i.e. they run get_db('foo'); ...; get_db('foo'). In fact that is suggested usage. It is possible
// that they do not register validation functions the second time, or that they register them differently. If that happens, there are two
// ways to fail here:
//
// 1. Either the user thinks validation is happening, but it is not, or
// 2. The user actually intends different validation for each instance, but we cache the validation functions and overzealously apply it
//
// Both are bad, but case 1 is worse. Therefore, for a given prefix + DB name, we cache validation functions. The only thing the user can
// do is add more, but never take them away. To take them away, restart the program. Also, ddocs are persistent a database, so it makes sense
// that validation functions are too.
var VALIDATION_CACHE = {}


function random_uuid() {
  return PouchDB.utils.uuid()
}

function get_db(name, options, callback) {
  if (!callback && typeof options == 'function') {
    callback = options
    options = {}
  }

  if (typeof name != 'string')
    throw new Error(`Must provide a name: ${name}`)

  options = options || {}
  var opts = { db      : options.db       || leveldown
             , prefix  : options.prefix   || process.env.POUCHDB_PREFIX
             , ddocs   : options.ddocs    || []
             , validate: options.validate || null
             }

  if (! opts.prefix) {
    console.log('WARN: fun-pouchdb: No directory prefix specified; supply a .prefix option, or set $POUCHDB_PREFIX')
    console.log('WARN: fun-pouchdb: Fallback prefix: %s', PREFIX)
    opts.prefix = PREFIX
  }

  if (! opts.prefix.endsWith('/'))
    opts.prefix += '/'

  // It is not allowed to provide a different validation function if one has already been cached.
  var validation_function = get_validation_function(opts.prefix, name)
  if (validation_function && opts.validate && validation_function !== opts.validate)
    throw new Error(`Validation function already set for DB ${name}; cannot change it`)

  // If no validation function is cached, but one was provided in the options, then use that, and cache it.
  if (!validation_function && opts.validate)
    validation_function = set_validation_function(opts.prefix, name, opts.validate)
    
  // If no validation function was cached, and none is provided, warn the user.
  if (! validation_function)
    console.log('WARN: fun-pouchdb: DB %s has no validation function; be careful!', name)

  debug('Get DB: %j %j', name, opts)
  return new PouchDB(name, opts, function(er, db) {
    if (er)
      return callback(er)

    db._fun = {}
    db._fun.name = name
    db._fun.validate = validation_function

    db.bulkDocs = bulk_docs_validate

    prep_ddocs(db, opts.ddocs, function(er) {
      callback(er, db)
    })
  })
}

function set_validation_function(prefix, db_name, func) {
  var key = prefix + ':' + db_name
  if (VALIDATION_CACHE[key])
    throw new Error(`Validation function for DB ${key} already exists`)

  debug('Set validation function: %s', key)
  VALIDATION_CACHE[key] = func

  return func
}

function get_validation_function(prefix, db_name) {
  var key = prefix + ':' + db_name
  return VALIDATION_CACHE[key]
}
