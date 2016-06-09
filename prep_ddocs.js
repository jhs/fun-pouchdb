module.exports = prep_ddocs

var async = require('async')
var debug = require('debug')('fun-pouchdb:prep-ddocs')

function prep_ddocs(db, ddocs, callback) {
  var db_name = db._fun.name
  debug('Prepare %s design documents for DB: %s', ddocs.length, db_name)
  async.forEach(ddocs, prep_ddoc, ddocs_prepared)

  function prep_ddoc(ddoc, to_async) {
    debug('Prepare ddoc: %s/%s', db_name, ddoc._id)
    db.txn({id:ddoc._id, create:true}, populate_ddoc, ddoc_stored)

    function populate_ddoc(doc, to_txn) {
      for (var key in ddoc)
        doc[key] = ddoc[key]

      var views = doc.views || {}
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
      debug('Prepared ddoc %s/%s: %s', db_name, new_ddoc._id, result)
      return to_async(null)
    }
  }

  function ddocs_prepared(er) {
    if (er)
      return callback(er)

    debug('Completed preparing %s design documents in DB: %s', ddocs.length, db_name)
    callback()
  }
}


