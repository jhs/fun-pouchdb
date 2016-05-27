module.exports = get_db
module.exports.uuid = random_uuid

var txn = require('txn')
var debug = require('debug')('fun-pouchdb')
var PouchDB = require('pouchdb')
var leveldown = require('leveldown')

var validate = require('./validate.js')

var PREFIX = __dirname + '/../../db/_pouch/'
var CACHE = {}

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
  var opts = { db    : options.db     || leveldown
             , prefix: options.prefix || process.env.POUCHDB_PREFIX
             }

  if (! opts.prefix) {
    console.log('WARN: fun-pouchdb: No directory prefix specified; supply a .prefix option, or set $POUCHDB_PREFIX')
    console.log('WARN: fun-pouchdb: Fallback prefix: %s', PREFIX)
    opts.prefix = PREFIX
  }

  debug('Get %j %j', name, opts)
  return new PouchDB(name, opts, function(er, db) {
    if (er)
      return callback(er)

    db.bulkDocs = bulkDocsValidate
    prep_ddocs(db, name, function(er) {
      callback(er, db)
    })
  })
}

// Load a plugin into PouchDB to validate all writes.
var bulkDocs = PouchDB.prototype.bulkDocs
//PouchDB.plugin({bulkDocs: bulkDocsValidate})
PouchDB.plugin(txn.PouchDB)

function bulkDocsValidate(body, options, callback) {
  if (!callback && typeof options == 'function') {
    callback = options
    options = {}
  }

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

// Add any design documents with views, etc. needed
function prep_ddocs(db, name, callback) {
  if (name == 'product')
    return prep_product_ddocs(db, done)
  else if (name == 'customer')
    return prep_customer_ddocs(db, done)
  else if (name == 'cart')
    return prep_cart_ddocs(db, done)
  else if (name == 'coupon')
    return prep_coupon_ddocs(db, done)
  else if (name == 'order')
    return prep_order_ddocs(db, done)

  debug('No ddocs for db: %s', name)
  return callback(null)

  function done(er, doc, txr) {
    if (er)
      return callback(er)

    var result = (txr.stores == 0) ? 'No change' : 'Updated'
    debug('Prepared ddoc %s/%s: %s', name, doc._id, result)
    callback(null)
  }
}

function prep_product_ddocs(db, callback) {
  db.txn({id:'_design/product', create:true}, product_ddoc, callback)

  function product_ddoc(ddoc, to_txn) {
    ddoc.views = {
      category: {
        map: function(doc) {
          var cats = doc.categories || []
          for (var i = 0; i < cats.length; i ++)
            if (cats[i].category_id)
              emit(cats[i].category_id, doc)
        }.toString()
      },

      in_stock: {
        reduce: '_count',
        map: function(doc) {
          if (doc.stock_data && doc.stock_data.is_in_stock === '0')
            emit(false, doc.sku)
          else if (doc.stock_data && doc.stock_data.is_in_stock === '1')
            emit(true, doc.sku)
          else
            emit('error', doc.sku)
        }.toString()
      },

      stock_quantity: {
        reduce: '_sum',
        map: function(doc) {
          var stock = doc.stock_data
          if (stock && stock.qty && stock.is_in_stock === '1')
            emit(+stock.qty, +stock.qty)
        }.toString()
      },

      sku: {
        map: function(doc) {
          var sku = doc.sku
          if (typeof sku == 'string')
            emit(doc.sku, doc)
        }.toString()
      },

      url_key: {
        map: function(doc) {
          if (doc.type == 'product' && typeof doc.url_key == 'string')
            emit(doc.url_key, doc)
        }.toString()
      }
    }
    return to_txn()
  }
}

function prep_customer_ddocs(db, callback) {
  db.txn({id:'_design/customer', create:true}, make_ddoc, callback)

  function make_ddoc(ddoc, to_txn) {
    ddoc.views = {
      email: {
        map: function(doc) {
          if (doc.email)
            emit(doc.email, doc)
        }.toString()
      },

      name: {
        map: function(doc) {
          var vals = [doc.firstname, doc.middlename, doc.lastname]
          for (var i = 0; i < vals.length; i++) {
            var val = vals[i]
            if (val && typeof val == 'string') {
              var parts = val.trim().split(/ +/)
              parts.forEach(function(part) {
                //console.log('emit: %s', part.toLowerCase())
                emit(part.toLowerCase(), doc)
              })
            }
          }
        }.toString()
      },

      facebook_id: {
        map: function(doc) {
          var id = doc.facebook && doc.facebook.id
          if (id)
            emit(id, doc)
        }.toString()
      },

      gender: {
        reduce: '_count',
        map: function(doc) {
          if (doc.gender)
            emit(doc.gender, doc)
        }.toString()
      }
    }
    return to_txn()
  }
}

function prep_cart_ddocs(db, callback) {
  db.txn({id:'_design/cart', create:true}, prep, callback)

  function prep(ddoc, to_txn) {
    ddoc.views = {
      // Magento states:
      // 1. live - Cart is still active (in principle); not checked out yet.
      // 2. ready - Cart is ready to become a Magento order.
      // 3. complete - Cart has become a magento order.
      // * or any "error" state, to sideline a doc from the workflow
      magento_state: {
        map: function(doc) {
          if (doc.error)
            emit('error', doc)
          else if (!doc.is_checkout)
            emit('live', doc)
          else if (!doc.is_complete)
            emit('ready', doc)
          else
            emit('complete', doc)
        }.toString()
      },
      order_number: {
        map: function(doc) {
          var order_number = doc.order_number
          if (typeof order_number == 'string')
            emit(doc.order_number, doc)
        }.toString()
      }
    }

    return to_txn()
  }
}

function prep_coupon_ddocs(db, callback) {
  db.txn({id:'_design/coupon', create:true}, prep, callback)

  function prep(ddoc, to_txn) {
    ddoc.views = {
      coupon_code: {
        map: function(doc) {
          if (doc.type == 'coupon' && doc.couponCode && doc.isActive == "1")
            emit(doc.couponCode, doc)
        }.toString()
      }
    }

    return to_txn()
  }
}

function prep_order_ddocs(db, callback) {
  db.txn({id:'_design/order', create:true}, prep, callback)

  function prep(ddoc, to_txn) {
    ddoc.views = {
      created_at: {
        map: function(doc) {
          //console.log('Sale view %s: %s', doc._id, doc.status)
          if (doc.type == 'order') {
            if (doc.status == 'pending' || doc.status == 'complete') {
              // The trick is to get the "sale" time. The created_at and updated_at timestamps are not quite right.
              var ts = doc.created_at
              var comments = doc.order_comments || []
              for (var comment of comments)
                if (comment.status == 'pending')
                  ts = new Date(comment.created_at + 'Z').toJSON()
              emit(ts, doc)
            }
          }
        }.toString(),
        reduce: '_count'
      }
    }

    return to_txn()
  }
}
