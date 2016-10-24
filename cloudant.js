module.exports = { cloudant: sync_with_cloudant }

var debug = require('debug')('fun-pouchdb:cloudant')
var EventEmitter = require('events').EventEmitter


var DEFAULT_TIMEOUT = 10 * 1000
var DEFAULT_BATCH_SIZE = 100

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

    var timeout = options.timeout    || DEFAULT_TIMEOUT
    var batch   = options.batch_size || DEFAULT_BATCH_SIZE

    debug('Begin Cloudant sync batch_size=%j timeout=%j', batch, timeout)
    db.cloudant_pull = db.replicate.from(cloudant_url, { batch_size:batch, live:true, retry:true, timeout:timeout })
    db.cloudant_push = db.replicate.to(cloudant_url  , { batch_size:batch, live:true, retry:true, timeout:timeout
                                                       , filter:block_ddocs_by_default})

    // A "sync" is actually just a push and a pull happening at the same time. Mostly, they run independently of each
    // other except that a doc written by the pull will trigger that doc to go back through the push, and vice versa.
    // In both cases, there is a no-op because the remote DB obviously has that revision already.
    //
    // Anyway, there is no great way to detect a "sync" event. But one quick heuristic is when both replications pause.
    // In practice, we could look at the timing, and compare changes feeds but this is a good start.
    db.fun.sync = new EventEmitter
    var is_paused = {push:false, pull:false}
    function activity(direction, type) {
      var replicator = (direction == 'pull') ? db.cloudant_pull : db.cloudant_push

      is_paused[direction] = (type == 'paused')

      if (is_paused.push && is_paused.pull) {
        debug('Looks like the sync is done for now')
        db.fun.sync.emit('sync')
      }

    }

    db.cloudant_pull
      .on('denied', handle_error)
      .on('error' , handle_error)
      .on('change', report_change(db, 'Pull'))
      .on('active', function() {
        debug('Pull started: %s.cloudant.com/%s', options.account, name)
        activity('pull', 'active')
      })
      .on('paused', function(er) {
        debug('Pull paused')
        activity('pull', 'paused')
      })
      .on('complete', function(info) {
        var time = duration_label(new Date(info.start_time), new Date(info.end_time))
        debug(`Pulled: ${info.docs_read}/${info.docs_written} read/written in ${time}`)
        activity('pull', 'complete')
      })

    db.cloudant_push
      .on('denied', handle_error)
      .on('error' , handle_error)
      .on('change', report_change(db, 'Push'))
      .on('active', function() {
        debug('Push started: %s.cloudant.com/%s', options.account, name)
        activity('push', 'active')
      })
      .on('paused', function(er) {
        debug('Push paused')
        activity('push', 'paused')
      })
      .on('complete', function(info) {
        activity('push', 'paused')
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

    function handle_error(er) {
      if (er && er.name == 'compilation_error')
        return db.emit('warn', 'Design document compilation error: ' + er.reason)

      // Otherwise forward the error through.
      setImmediate(function() {
        db.emit('error', er)
      })
    }
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
