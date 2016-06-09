# Fun PouchDB

Thin wrapper around PouchDB for convenient server use

The common scenario is, you have a Node.js application, usually a server. You need just a few basic things:

1. Open some databases by name
2. Let me specify any design docs I need, as a JavaScript object
3. Let me provide a simple throws/doesn't-throw function to validate a doc

Usage:

``` js
var DB = require('fun-pouchdb').defaults({prefix: __dirname})

DB('my_db', function(er, my_db) {
  console.log('At this point, my_db is a plain PouchDB object')
})
```

Or as a list of names to open at once.

``` js
DB(['users', 'web', 'billing'], function(er, dbs) {
  // The databases are in the dbs object, by name.
  dbs.users.get('some_doc', function() { /* etc */ })
  dbs.web
  dbs.billing // etc.
})
```

However, the most common thing to do in a real world application is to provide a bunch of names, plus design docs and validation functions. Basically, you have a big definition of all your databases, plus the design docs you need (for views), plus validation functions. Placing this all together in Node.js code is a great way to ensure that it is well-tested.

``` js
// Define all the DBs needed for this app. The key is the database name, and
// the value is the options for that database.
var opts = {}

// A "ddoc" option will ensure that design document will be in the database.
opts.users = { ddoc: {_id:'_design/foo', views: {}} }

// Or provide several ddocs in an array.
opts.web = {
  ddocs: [
    // First one
    {_id:'_design/foo', views: {}},

    // Second one. Note, the map function is a real function.
    { _id:'_design/bar',
      views: {
        by_name: {
          map: function(doc) { emit(doc.name) }
        }
      }
    }
  ]
}

// In reality, you will want at least a simple pass/fail validation function,
// to prevent a bug or something getting a bad document into the DB.
opts.web.validate = check_web_document

// This doc checker simply throws an Error if anything looks wrong. You can call
// console.log() or logging.
//
// Of course, you export this function and write lots of good unit tests.
function check_web_document(doc) {
  console.log('Checking web document: %s', doc._id)

  if (doc.type != 'web')
    throw new Error(`Document type must be "web": ${doc._id}`)

  if (doc.a && doc.b && doc.c && doc.a + doc.b != doc.c)
    throw new Error(`Bad arithmetic A plus B should equal C`)

  // Et cetera.
}
```

Now send that to DB, and you will get the same friendly object full of PouchDB instances, but with all of your design documents and validation in place.

``` js
DB(opts, function(er, dbs) {
  dbs.users.put({name:'John Doe'}, function(er, response) {
    console.log('Stored a doc in the users DB')
  })

  // This will fail the A+B=C validation test.
  dbs.web.put({type:'web', a:5, b:3, c:53}, function(er, response) {
    console.log('Error: %s', response.name) // Error: forbidden
    console.log(response.exception) // Prints the full Error object
  })
})
```
