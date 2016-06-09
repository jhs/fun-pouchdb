module.exports = bulkDocsValidate

var debug = require('debug')('fun-pouchdb:validation')
var PouchDB = require('pouchdb')

// Overwriting bulkDocs via a plugin does not work for some reason that I cannot remember.
var bulkDocs = PouchDB.prototype.bulkDocs

function bulkDocsValidate(body, options, callback) {
  if (!callback && typeof options == 'function') {
    callback = options
    options = {}
  }

  var self = this
  var validate = this.validate || this._fun.validate

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
    try {
      if (validate)
        validate.call(self, doc)
    } catch (er) {
      failure = er
    }

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
