var fs = require('fs')
var path = require('path')
var walker = require('folder-walker')
var mm = require('musicmetadata')
var pump = require('pump')
var validExtensions = ['m4a', 'mp3']
var ndjson = require('ndjson')
var filter = require('through2-filter')
var through = require('through2')
var xtend = require('xtend')
var pumpify = require('pumpify')
var level = require('level')
var byteStream = require('byte-stream')
var LevelBatch = require('level-batch-stream')
var map = require('through2-map')
var bytewise = require('bytewise')
var parallel = require('concurrent-writable')
var count = 0
var spy = require('through2-spy').objCtor(obj => {
  count++
  console.dir(obj, {colors: true})
})

var db = level('./hyperamp-library', {
  keyEncoding: bytewise,
  valueEncoding: 'json'
})
var libPath = '/Volumes/uDrive/Plex/Music'

var fileStream = walker([libPath])
var filterInvalid = filter.obj(isValidFile)
var filterAdded = through.obj(dbStat)

var levelBatch = new LevelBatch(db)
var makeBatch = map.obj((chunk) => ({
  type: 'put',
  key: [chunk.relname.toLowerCase(), chunk.filepath],
  value: chunk
}))
// Pretty good, but need to tune
var batcher = byteStream({time: 200, limit: 100})
var paralleLevelBatch = parallel(levelBatch, 10)

function addNew (cb) {
  pump(
    fileStream,
    filterInvalid,
    filterAdded,
    parseMetaData(),
    makeBatch,
    batcher,
    spy(),
    paralleLevelBatch,
    cb
  )
}

function printDb (cb) {
  pump(
    db.createKeyStream(),
    spy(),
    terminateObjStream(),
    cb
  )
}

printDb(done)
// addNew(done)

function terminateObjStream () {
  return pumpify.obj(ndjson.serialize(), fs.createWriteStream('/dev/null'))
}

function done (err) {
  if (err) throw err
  console.log('done!')
  console.log(count)
}

function isValidFile (data) {
  if (data.type !== 'file') return false
  let ext = path.extname(data.basename).substring(1)
  return validExtensions.includes(ext)
}

function dbStat (chunk, enc, cb) {
  db.get(chunk.filepath, (err, value) => {
    if (err && err.notFound) {
      this.push(chunk)
      return cb()
    }
    return cb(err)
  })
}

function parseMetaData () {
  function parser (chunk, enc, cb) {
    parseMetadata(chunk, (err, meta) => {
      if (err) return cb(err)
      delete meta.picture
      this.push(xtend(chunk, {meta: meta}))
      cb()
    })
  }

  return through.obj(parser)
}

function parseMetadata (data, cb) {
  var { filepath } = data
  var fileStream = fs.createReadStream(filepath)
  mm(fileStream, { duration: true }, (err, meta) => {
    fileStream.close()
    if (!meta) meta = {}
    if (err) {
      err.message += ` (file: ${filepath})`
      meta.error = err
    }

    if (!meta.title) {
      let { basename } = data
      let ext = path.extname(basename)
      meta.title = path.basename(basename, ext)
    }

    cb(null, meta)
  })
}
