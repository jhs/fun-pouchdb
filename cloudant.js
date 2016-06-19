module.exports = { cloudant: sync_with_cloudant }

var debug = require('debug')('fun-pouchdb:cloudant')

function sync_with_cloudant(options) {
  var db = this
  var name = db.fun.name

  var cloudant_url = `https://${options.account}:${options.password}@${options.account}.cloudant.com/${name}`

  // Give the user some useful utility functions.
  db.fun.cloudant.offline = go_offline
  db.fun.cloudant.online  = go_online
  db.fun.cloudant.edit = edit_url
  db.fun.cloudant.state = options.state || null

  // Auto-sync by default.
  if (db.fun.cloudant.state == 'online') {
    debug('User set state to "online"; begin automatic sync')
    go_online()
  } else if (db.fun.cloudant.state) {
    debug('User set state: %j; no automatic sync', options.state)
  } else {
    debug('User did not set state; go online automatically')
    go_online()
  }
  
  function edit_url(id) {
    return `https://${options.account}.cloudant.com/dashboard.html#/database/${name}/${encodeURIComponent(id)}`
  }

  function go_offline() {
    debug('Offline mode; cancel replications')
    db.fun.cloudant.state = 'offline'

    if (db.cloudant_pull && db.cloudant_pull.cancel)
      db.cloudant_pull.cancel()
    if (db.cloudant_push && db.cloudant_push.cancel)
      db.cloudant_push.cancel()
  }

  function go_online() {
    debug('Online mode; start replications')
    db.fun.cloudant.state = 'online'

    if (db.cloudant_pull && db.cloudant_pull.cancel) {
      debug('Cancel old pull replication before going online')
      db.cloudant_pull.cancel()
    }
    if (db.cloudant_push && db.cloudant_push.cancel) {
      debug('Cancel old push replication before going online')
      db.cloudant_push.cancel()
    }

    debug('Begin Cloudant sync')
    db.cloudant_pull = db.replicate.from(cloudant_url, {batch_size:1000, live:true, retry:true})
    db.cloudant_push = db.replicate.to(cloudant_url, {batch_size:1000, live:true, retry:true, filter:block_ddocs_by_default})

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
  } // go_online
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

// Usually disallows design documents from replicating, unless they have .on_cloudant = true.
function block_ddocs_by_default(doc) {
  var match = doc._id.match(/^_design\//)
  if (! match)
    return true // Normal docs are fine.

  return !! doc.on_cloudant
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
