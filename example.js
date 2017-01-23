var fs = require('fs')
var path = require('path')
var walker = require('folder-walker')
var mm = require('musicmetadata')
var pump = require('pump')
var ndjson = require('ndjson')
var filter = require('through2-filter')
var through = require('through2')
var xtend = require('xtend')
var pumpify = require('pumpify')
var level = require('level')
var byteStream = require('byte-stream')
var LevelBatch = require('level-batch-stream')
var Idx = require('level-idx')
var map = require('through2-map')
// var indexer = require('level-indexer')
var bytewise = require('bytewise')
var parallel = require('concurrent-writable')
var sub = require('subleveldown')
var spy = require('through2-spy').objCtor(obj => {
  console.dir(obj, {colors: true})
})

var db = level('./db')
var idb = level('./idb')

var files = sub(db, 'fileds', {
  keyEncoding: bytewise,
  valueEncoding: 'json'
})

Idx(files, idb, {keyEncoding: bytewise})
  .by('AlbumArtistYear', [
    'meta.albumartist',
    'meta.artist',
    'meta.year',
    'meta.album',
    'meta.disk.no',
    'meta.track.no',
    'meta.title',
    'filepath'
  ])

// var index = indexer(db, ['artist', 'track'])
var libPath = '/Volumes/uDrive/Plex/Music'
var fileStream = walker([libPath])
var filterInvalid = filter.obj(isValidFile)
var filterAdded = through.obj(dbStat)
// var printIndex = through.obj(idxPrint)

var levelBatch = new LevelBatch(files)
var makeBatch = map.obj((chunk) => ({
  type: 'put',
  key: [chunk.filepath],
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
    spy(),
    makeBatch,
    batcher,
    paralleLevelBatch,
    cb
  )
}

// function printDb (cb) {
//   pump(
//     files.createValueStream(),
//     // spy(),
//     terminateObjStream(),
//     cb
//   )
// }

function printIndex (cb) {
  pump(
    files.byAlbumArtistYear.createValueStream(),
    // spy(),
    terminateObjStream(),
    cb
  )
}

addNew(done)
// printDb(done)
// addNew(function (err) {
//   if (err) throw err
//   printIndex(done)
// })

// printIndex(done)

function terminateObjStream () {
  return pumpify.obj(ndjson.serialize(), fs.createWriteStream('/dev/null'))
}

function done (err) {
  if (err) throw err
  console.log('done!')
  printIndex(() => { console.log('really done') })
}

var validExtensions = ['m4a', 'mp3']
function isValidFile (data) {
  if (data.type !== 'file') return false
  let ext = path.extname(data.basename).substring(1)
  return validExtensions.includes(ext)
}

// function idxPrint (chunk, enc, cb) {
//   var idx = index.key(chunk.value, chunk.key.join('!'))
//   console.log(idx)
//   this.push(chunk)
//   cb()
// }

function dbStat (chunk, enc, cb) {
  files.get([chunk.filepath], (err, value) => {
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
