module.exports = { cloudant: sync_with_cloudant }

var debug = require('debug')('fun-pouchdb:cloudant')

function sync_with_cloudant(options) {
  var db = this
  var name = db.fun.name

  // Give the user a quick way to call up a URL to edit a document.
  db.fun.cloudant.edit = function(id) {
    return `https://${options.account}.cloudant.com/dashboard.html#/database/${name}/${encodeURIComponent(id)}`
  }

  var cloudant_url = `https://${options.account}:${options.password}@${options.account}.cloudant.com/${name}`
  var opts = { batch_size:1000, filter:is_not_ddoc, live:true, retry:true }

  debug('Begin push replication')
  db.cloudant_pull = db.replicate.from(cloudant_url, {batch_size:1000, live:true, retry:true})
  db.cloudant_push = db.replicate.to(cloudant_url, {batch_size:1000, live:true, retry:true, filter:is_not_ddoc})

  db.cloudant_pull
    .on('active', function() { debug('Pull started: %s.cloudant.com/%s', options.account, name) })
    .on('denied', function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('error' , function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('paused', function(er) { debug('Pull paused') })
    .on('change', report_change(db, 'Pull'))
    .on('complete', function(info) {
      var time = duration_label(new Date(info.start_time), new Date(info.end_time))
      debug(`Pulled: ${info.docs_read}/${info.docs_written} read/written in ${time}`)
    })

  db.cloudant_push
    .on('active', function() { debug('Push started: %s.cloudant.com/%s', options.account, name) })
    .on('denied', function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('error' , function(er) { setImmediate(function() { db.emit('error', er) }) })
    .on('paused', function(er) { debug('Push paused') })
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

function duration_label(start, end) {
  var ms = end - start
  var seconds = ms / 1000
  var minutes = seconds / 60
  seconds = Math.round((minutes % 1) * 60)
  minutes = Math.floor(minutes)
  if (seconds < 10)
    seconds = '0' + seconds
  return `${minutes}:${seconds}`
}
