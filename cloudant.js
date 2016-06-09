module.exports = { cloudant: sync_with_cloudant }

var debug = require('debug')('fun-pouchdb:cloudant')

function sync_with_cloudant(options) {
  debug('sync_with_cloudant: %j', options)
  var db = this
  var name = db._fun.name

  var cloudant_url = `https://${options.account}:${options.password}@${options.account}.cloudant.com/${name}`
  var opts = { batch_size:1000, filter:is_not_ddoc, live:true, retry:true }

  debug('Begin push replication')
  return db.replicate.to(cloudant_url, {batch_size:1000, filter:is_not_ddoc})
    .on('active', function() { debug('Begin push: %s', target) })
    .on('denied', function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('error' , function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('paused', function(er) { debug('Pause: %j', er) })
    .on('change', report_change(db, 'Push'))
    .on('complete', function(info) {
      if (info.errors.length > 0) {
        for (var er of info.errors)
          db.emit('error', new Error(JSON.stringify(er)))
        return
      }

      if (info.doc_write_failures > 0)
        return db.emit('error', new Error(`Document write failures during push: ${info.doc_write_failures}`))
      if (! info.ok)
        return db.emit('error', new Error(`Unknown push error: ${JSON.stringify(info)}`))

      var time = duration_label(new Date(info.start_time), new Date(info.end_time))
      debug(`Pushed: ${info.docs_read}/${info.docs_written} read/written in ${time}`)
    })
}

//      function pull(target, cb) {
//        var url = `https://${target}:${ARGV.pw}@${target}.cloudant.com/${name}`
//        return db.replicate.from(url, {batch_size:1000})
//          .on('active', function() { console.log('Begin pull: %s', target) })
//          .on('denied', function(er) { setImmediate(function() { throw er }) })
//          .on('error' , function(er) { setImmediate(function() { throw er }) })
//          .on('change', report_change('Pull'))
//          .on('complete', function(info) {
//            cb({'Pulled    ': JSON.stringify(info)})
//          })
//      }
//
function report_change(db, label) {
  return reporter
  function reporter(status) {
    if (! status.ok)
      db.emit('error', new Error(`Error in ${label} replication: ${JSON.stringify(status)}`))
    if (status.errors.length > 0)
      db.emit('error', new Error(`Error in ${label} replication: ${JSON.stringify(status.errors)}`))

    debug('%s: read %s; written %s', label, status.docs_read, status.docs_written)
  }
}

//
// Miscellaneous
//

// Return whether a document is a normal document.
function is_not_ddoc(doc) {
  return !is_ddoc(doc)
}

// Return whether a document is a design document.
function is_ddoc(doc) {
  return !! doc._id.match(/^_design\//)
}
