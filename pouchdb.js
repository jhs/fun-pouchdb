module.exports = get_db
module.exports.uuid = random_uuid

var txn = require('txn')
var async = require('async')
var debug = require('debug')('fun-pouchdb')
var PouchDB = require('pouchdb')
var leveldown = require('leveldown')

var PREFIX = __dirname + '/../../db/_pouch/'

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

  // It is not allowed to provide a different validation function if one has already been cached.
  var validation_function = get_validation_function(opts.prefix, name)
  if (validation_function && opts.validate && validation_function !== opts.validate)
    throw new Error(`Validation function already set for DB ${name}; cannot change it`)

  // If no validation function is cached, but one was provided in the options, then use that, and cache it.
  if (!validation_function && opts.validate)
    validation_function = set_validation_function(opts.prefix, name, opts.validate)
    
  // If no validation function was cached, and none is provided, warn the user.
  if (! validation_function)
    console.log('WARN: fun-pouchdb: DB %s has no validation function; be careful!')

  debug('Get DB: %j %j', name, opts)
  return new PouchDB(name, opts, function(er, db) {
    if (er)
      return callback(er)

    db.bulkDocs = bulkDocsValidate

    db._fun = {}
    db._fun.name = name
    db._fun.validate = validation_function

    prep_ddocs(db, opts.ddocs, function(er) {
      callback(er, db)
    })
  })
}

function prep_ddocs(db, ddocs, callback) {
  var db_name = this._fun.name
  debug('Prepare %s design documents for DB: %s', ddocs.length, db_name)
  async.forEach(ddocs, prep_ddoc, ddocs_prepared)

  function prep_ddoc(ddoc, to_async) {
    debug('Prepare ddoc: %s/%s', db_name, ddoc._id)
    db.txn({id:ddoc._id, create:true}, populate_ddoc, ddoc_stored)

    function populate_ddoc(doc, to_txn) {
      for (var key in ddoc)
        doc[key] = ddoc[key]

      var views = doc.views
      for (var view_name in views) {
        var view = views[vew_name]

        if (typeof view.map == 'function')
          view.map = view.map.toString()
        if (typeof view.reduce == 'function')
          view.reduce = view.reduce.toString()
      }

      return to_txn()
    }

    function ddoc_stored(er, new_ddoc, txr) {
      if (er)
        return to_async(er)

      var result = (txr.stores == 0) ? 'No change' : 'Updated'
      debug('Prepared ddoc %s/%s: %s', db_name, doc._id, result)
      return to_async(null)
    }
  }

  function ddocs_prepared(er) {
    if (er)
      return callback(er)

    debug('Completed preparing %s design documents in DB: %s', this._fun.name)
    callback()
  }
}


// Overwriting bulkDocs via a plugin does not work for some reason that I cannot remember.
var bulkDocs = PouchDB.prototype.bulkDocs

function bulkDocsValidate(body, options, callback) {
  if (!callback && typeof options == 'function') {
    callback = options
    options = {}
  }

  var validate = this._fun.validate || noop

  if (Array.isArray(body))
    var docs = body
  else
    var docs = body.docs

  // Validating is easy enough. The problem is to return the result array appropriate to body.docs. In other words:
  // bulkDocs([valid, invalid, valid], ...) should return a 3-array [ {ok:true}, {error:forbidden}, {ok:true} ], with
  // the middle one being the validation failure. But we cannot really send that invalid doc to PouchDB.bulkDocs().
  //
  // So the solution is to keep an array of invalid docs, and then to rebuild the result array once bulkDocs executes.
  var validDocs = []
  var result = []
  for (var i = 0; i < docs.length; i++) {
    var doc = docs[i]

    var failure = null
    try { validate(doc) }
    catch (er) { failure = er }

    if (failure)
      result.push({error:true, status:403, name:'forbidden', message:failure.message, exception:failure})
    else {
      result.push(null)
      validDocs.push(doc) // Send onward to bulkDocs().
    }
  }

  // Now validDocs is a shorter list of valid documents. result is the complete list, either null or the validation error.
  return bulkDocs.call(this, {docs:validDocs}, options, function(er, dbResult) {
    if (er)
      return callback(er)

    // Populate the full result with the respective results for valid documents.
    for (var i = 0; i < result.length; i++) {
      if (!result[i]) {
        // This document was valid. Insert its result into the result list.
        result[i] = dbResult.shift()
        if (!result[i])
          return callback(new Error('Unknown error building bulkDocs result, ran out of DB results'))
      }
    }

    if (dbResult.length != 0)
      return callback(new Error('Unknown error building bulkDocs result, too many DB results'))

    debug('bulkDocsValidate complete; %s/%s valid docs', validDocs.length, result.length)
    callback(null, result)
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

function noop() {
}
